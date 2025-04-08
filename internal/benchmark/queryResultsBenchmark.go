package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/lansweeper/ClickhouseBenchTool/internal/suite"
)

const QueryResultsBenchmarkName = "queryResultsBenchmark"

type QueryResultsChResults struct {
	IsEqual bool
}

type QueryResultsBenchmark struct {
	conn clickhouse.Conn
}

func NewQueryResultsBenchmark(conn clickhouse.Conn) *QueryResultsBenchmark {
	return &QueryResultsBenchmark{
		conn: conn,
	}
}

func (qlb *QueryResultsBenchmark) Name() string {
	return QueryResultsBenchmarkName
}

func (qlb *QueryResultsBenchmark) Run(ctx context.Context, queryParams map[string]any, queryName string, query string) (map[string]string, error) {
	queryId := uuid.New()

	params := []driver.NamedValue{}
	for k, v := range queryParams {
		params = append(params, clickhouse.Named(k, v))
	}
	interfaceParams := make([]interface{}, len(params))
	for i, v := range params {
		interfaceParams[i] = v
	}

	rows, err := qlb.conn.Query(clickhouse.Context(ctx, clickhouse.WithQueryID(queryId.String())), query, interfaceParams...)
	if err != nil {
		return nil, fmt.Errorf("run query log benchmark: %w", err)
	}

	var columnTypes = rows.ColumnTypes()
	var result []interface{}
	for rows.Next() {
		vars := make([]interface{}, len(columnTypes))
		for i := range columnTypes {
			vars[i] = reflect.New(columnTypes[i].ScanType()).Interface()
		}
		err := rows.Scan(vars...)
		if err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		result = append(result, vars...)
	}

	resultsJson, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}

	return map[string]string{
		"numResults":  fmt.Sprintf("%d", len(result)),
		"resultsJson": string(resultsJson),
	}, nil
}

func (qlb *QueryResultsBenchmark) OnModuleEnd(results suite.BenchmarkResults) (map[string]string, error) {
	areEqual := true
	lastNumResults := 0
	lastResults := ""
	for i, result := range results {
		if i == 0 {
			var err error
			lastNumResults, err = strconv.Atoi(result["numResults"])
			if err != nil {
				return nil, fmt.Errorf("parse numResults: %w", err)
			}
			lastResults = result["resultsJson"]
			continue
		}

		numResults, err := strconv.Atoi(result["numResults"])
		if err != nil {
			return nil, fmt.Errorf("parse numResults: %w", err)
		}
		if numResults != lastNumResults || result["resultsJson"] != lastResults {
			areEqual = false
			break
		}
	}

	fmt.Println("Results are equal: ", areEqual)
	return map[string]string{
		"areEqual": fmt.Sprintf("%t", areEqual),
	}, nil
}
