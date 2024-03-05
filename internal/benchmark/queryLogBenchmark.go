package benchmark

import (
	"context"
	"fmt"
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/lansweeper/ClickhouseBenchTool/internal"
)

func RunQueryLogBenchmark(ctx context.Context, conn clickhouse.Conn, query string, queryParams map[string]any) (internal.QueryLogBenchmarkResults, error) {
	queryId := uuid.New()
	queryLogResults := internal.QueryLogBenchmarkResults{}

	params := []driver.NamedValue{}
	for k, v := range queryParams {
		params = append(params, clickhouse.Named(k, v))
	}
	interfaceParams := make([]interface{}, len(params))
	for i, v := range params {
		interfaceParams[i] = v
	}

	rows, err := conn.Query(clickhouse.Context(ctx, clickhouse.WithQueryID(queryId.String())), query, interfaceParams...)
	if err != nil {
		return queryLogResults, fmt.Errorf("run query log benchmark: %w", err)
	}

	var columnTypes = rows.ColumnTypes()
	for rows.Next() {
		vars := make([]interface{}, len(columnTypes))
		for i := range columnTypes {
			vars[i] = reflect.New(columnTypes[i].ScanType()).Interface()
		}
		err := rows.Scan(vars...)
		if err != nil {
			return queryLogResults, fmt.Errorf("scan row: %w", err)
		}
	}
	// Search query in query logs
	err = conn.Exec(ctx, "SYSTEM FLUSH LOGS ON CLUSTER default")
	if err != nil {
		return queryLogResults, fmt.Errorf("flush logs: %w", err)
	}
	row := conn.QueryRow(ctx, `
		SELECT
			read_bytes,
			result_bytes,
			read_rows,
			result_rows,
			memory_usage,
			ProfileEvents [ 'OSCPUVirtualTimeMicroseconds' ] as os_cpu_virtual_time_microseconds,
			query,
			query_id
		FROM
			clusterAllReplicas('default', system.query_log)
		WHERE
			type = 'QueryFinish'
			AND client_name LIKE 'clickhouse-benchmark-tool%%'
			AND query_id = $1`, queryId.String())

	err = row.ScanStruct(&queryLogResults)
	if err != nil {
		return queryLogResults, fmt.Errorf("scan query log row: %w", err)
	}

	return queryLogResults, nil
}
