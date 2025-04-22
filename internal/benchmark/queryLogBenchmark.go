package benchmark

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/lansweeper/ClickhouseBenchTool/internal/suite"
)

const QueryLogBenchmarkName = "queryLogBenchmark"

type QueryLogBenchmarkResultKeys string

const (
	LogQueryNameKey             QueryLogBenchmarkResultKeys = "query_name"
	ReadBytesKey                QueryLogBenchmarkResultKeys = "read_bytes"
	ReadBytesReadableKey        QueryLogBenchmarkResultKeys = "read_bytes_readable"
	ResultBytesKey              QueryLogBenchmarkResultKeys = "result_bytes"
	ResultBytesReadableKey      QueryLogBenchmarkResultKeys = "result_bytes_readable"
	ReadRowsKey                 QueryLogBenchmarkResultKeys = "read_rows"
	ResultRowsKey               QueryLogBenchmarkResultKeys = "result_rows"
	MemoryUsageKey              QueryLogBenchmarkResultKeys = "memory_usage"
	MemoryUsageReadableKey      QueryLogBenchmarkResultKeys = "memory_usage_readable"
	OSCPUVirtualTimeKey         QueryLogBenchmarkResultKeys = "os_cpu_virtual_time"
	OSCPUVirtualTimeReadableKey QueryLogBenchmarkResultKeys = "os_cpu_virtual_time_readable"
)

type queryLogChResults struct {
	ReadBytes                uint64 `ch:"read_bytes"`
	ReadBytesReadable        string `ch:"read_bytes_readable"`
	ResultBytes              uint64 `ch:"result_bytes"`
	ResultBytesReadable      string `ch:"result_bytes_readable"`
	ReadRows                 uint64 `ch:"read_rows"`
	ResultRows               uint64 `ch:"result_rows"`
	MemoryUsage              uint64 `ch:"memory_usage"`
	MemoryUsageReadable      string `ch:"memory_usage_readable"`
	OSCPUVirtualTime         uint64 `ch:"os_cpu_virtual"`
	OSCPUVirtualTimeReadable string `ch:"os_cpu_virtual_readable"`
}

type QueryLogBenchmark struct {
	conn clickhouse.Conn
}

func NewQueryLogBenchmark(conn clickhouse.Conn) *QueryLogBenchmark {
	return &QueryLogBenchmark{
		conn: conn,
	}
}

func (qlb *QueryLogBenchmark) Name() string {
	return QueryLogBenchmarkName
}

func (qlb *QueryLogBenchmark) Run(ctx context.Context, queryParams map[string]any, queryName string, query string) (map[string]string, error) {
	queryId := uuid.New()

	params := []driver.NamedValue{}
	for k, v := range queryParams {
		params = append(params, clickhouse.Named(k, v))
	}
	interfaceParams := make([]interface{}, len(params))
	for i, v := range params {
		interfaceParams[i] = v
	}

	_, err := qlb.conn.Query(clickhouse.Context(ctx, clickhouse.WithQueryID(queryId.String())), query, interfaceParams...)
	if err != nil {
		return map[string]string{}, fmt.Errorf("run query log benchmark: %w", err)
	}

	// Search query in query logs
	err = qlb.conn.Exec(ctx, "SYSTEM FLUSH LOGS ON CLUSTER default")
	if err != nil {
		return map[string]string{}, fmt.Errorf("flush logs: %w", err)
	}
	row := qlb.conn.QueryRow(ctx, `
		SELECT
			read_bytes,
			formatReadableSize(read_bytes) as read_bytes_readable,
			result_bytes,
			formatReadableSize(result_bytes) as result_bytes_readable,
			read_rows,
			result_rows,
			memory_usage,
			formatReadableSize(memory_usage) as memory_usage_readable,
			ProfileEvents [ 'OSCPUVirtualTimeMicroseconds' ] as os_cpu_virtual,
			formatReadableTimeDelta(ProfileEvents [ 'OSCPUVirtualTimeMicroseconds' ]/1000000, 'seconds', 'microseconds') as os_cpu_virtual_readable
		FROM
			clusterAllReplicas('default', system.query_log)
		WHERE
			type = 'QueryFinish'
			AND client_name LIKE 'clickhouse-benchmark-tool%%'
			AND query_id = $1`, queryId.String())

	var queryLogResults queryLogChResults
	err = row.ScanStruct(&queryLogResults)
	if err != nil {
		return map[string]string{}, fmt.Errorf("scan query log row: %w", err)
	}

	return map[string]string{
		string(LogQueryNameKey):             queryName,
		string(ReadBytesKey):                fmt.Sprintf("%v", queryLogResults.ReadBytes),
		string(ReadBytesReadableKey):        fmt.Sprintf("%v", queryLogResults.ReadBytesReadable),
		string(ResultBytesKey):              fmt.Sprintf("%v", queryLogResults.ResultBytes),
		string(ResultBytesReadableKey):      fmt.Sprintf("%v", queryLogResults.ResultBytesReadable),
		string(ReadRowsKey):                 fmt.Sprintf("%v", queryLogResults.ReadRows),
		string(ResultRowsKey):               fmt.Sprintf("%v", queryLogResults.ResultRows),
		string(MemoryUsageKey):              fmt.Sprintf("%v", queryLogResults.MemoryUsage),
		string(MemoryUsageReadableKey):      fmt.Sprintf("%v", queryLogResults.MemoryUsageReadable),
		string(OSCPUVirtualTimeKey):         fmt.Sprintf("%v", queryLogResults.OSCPUVirtualTime),
		string(OSCPUVirtualTimeReadableKey): fmt.Sprintf("%v", queryLogResults.OSCPUVirtualTimeReadable),
	}, nil
}

func (qlb *QueryLogBenchmark) OnModuleEnd(results suite.BenchmarkResults) (map[string]string, error) {
	return nil, nil
}
