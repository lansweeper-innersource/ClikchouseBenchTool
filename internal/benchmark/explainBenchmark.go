package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/PaesslerAG/gval"
	"github.com/PaesslerAG/jsonpath"
	"github.com/lansweeper/ClickhouseBenchTool/internal/suite"
)

type queryExplainResultData struct {
	Type     string
	Keys     []string
	Parts    [2]float64
	Granules [2]float64
}

type queryExplainBenchmarkData struct {
	MinMax     queryExplainResultData
	Partition  queryExplainResultData
	PrimaryKey queryExplainResultData
}

type ExplainBenchmarkResultKeys string

const (
	ExplainQueryKey       ExplainBenchmarkResultKeys = "query_name"
	MinMaxGranulesKey     ExplainBenchmarkResultKeys = "minMaxGranules"
	MinMaxPartsKey        ExplainBenchmarkResultKeys = "minMaxParts"
	MinMaxkeysKey         ExplainBenchmarkResultKeys = "minMaxkeys"
	PartitionGranulesKey  ExplainBenchmarkResultKeys = "partitionGranules"
	PartitionPartsKey     ExplainBenchmarkResultKeys = "partitionParts"
	PartitionkeysKey      ExplainBenchmarkResultKeys = "partitionkeys"
	PrimaryKeyGranulesKey ExplainBenchmarkResultKeys = "primaryKeyGranules"
	PrimaryKeyPartsKey    ExplainBenchmarkResultKeys = "primaryKeyParts"
	PrimaryKeykeysKey     ExplainBenchmarkResultKeys = "primaryKeykeys"
)

const ExplainBenchmarkName = "explainBenchmark"

type ExplainBenchmark struct {
	conn clickhouse.Conn
}

func NewExplainBenchmark(conn clickhouse.Conn) *ExplainBenchmark {
	return &ExplainBenchmark{
		conn: conn,
	}
}

func (qlb *ExplainBenchmark) Name() string {
	return ExplainBenchmarkName
}

func (qlb *ExplainBenchmark) Run(ctx context.Context, queryParams map[string]any, queryName string, query string) (map[string]string, error) {
	result := queryExplainBenchmarkData{}
	explainQuery := fmt.Sprintf("EXPLAIN indexes = 1, json = 1, description = 1 %s FORMAT TSVRaw", query)

	explainRow, err := qlb.conn.Query(ctx, explainQuery)
	if err != nil {
		return map[string]string{}, fmt.Errorf("run explain benchmark query: %w", err)
	}

	// The structure of the explain result is dynamic, depends on each query.
	// Reflection is used to extract the result and then unmarshal it to a map.
	columns := explainRow.ColumnTypes()
	rawResult := make([]interface{}, len(columns))
	for i := range columns {
		rawResult[i] = reflect.New(columns[i].ScanType()).Interface()
	}
	explainRow.Next()
	err = explainRow.Scan(rawResult...)
	if err != nil {
		return map[string]string{}, fmt.Errorf("scan explain query rows: %w", err)
	}
	explainRow.Close() // We now there is only one row

	explainResultBytes := []byte(reflect.ValueOf(rawResult[0]).Elem().String())
	resultsData := interface{}(nil)
	err = json.Unmarshal([]byte(explainResultBytes), &resultsData)
	if err != nil {
		return map[string]string{}, fmt.Errorf("unmarshal explain result: %w", err)
	}

	// Because the response is dynamic and complex the library jsonpath and gval are
	// used to extract the necessary information.
	builder := gval.Full(jsonpath.PlaceholderExtension())
	path, err := builder.NewEvaluable("$..Indexes")
	if err != nil {
		return map[string]string{}, fmt.Errorf("build jsonpath query to analyze explain query results: %w", err)
	}
	data, err := path(context.Background(), resultsData)
	if err != nil {
		return map[string]string{}, fmt.Errorf("execute jsonpath query to analyze explain query results: %w", err)
	}

	// The result is a nested list of maps, so it is necessary to flatten it and then
	// iterate over it to extract the necessary information.
	flatData := flatten(data.([]interface{}))
	for _, elem := range flatData {
		elemMap := elem.(map[string]interface{})
		var resultKeys []string
		if elemMap["Keys"] != nil {
			for _, key := range elemMap["Keys"].([]interface{}) {
				resultKeys = append(resultKeys, key.(string))
			}
		}
		resultsMap := queryExplainResultData{
			Type: elemMap["Type"].(string),
			Keys: resultKeys,
			Parts: [2]float64{
				elemMap["Initial Parts"].(float64),
				elemMap["Selected Parts"].(float64),
			},
			Granules: [2]float64{
				elemMap["Initial Granules"].(float64),
				elemMap["Selected Granules"].(float64),
			},
		}

		switch resultsMap.Type {
		case "MinMax":
			result.MinMax = resultsMap
		case "Partition":
			result.Partition = resultsMap
		case "PrimaryKey":
			result.PrimaryKey = resultsMap
		}
	}
	return map[string]string{
		string(ExplainQueryKey):       queryName,
		string(MinMaxGranulesKey):     joinNumberArray(result.MinMax.Granules[:]),
		string(MinMaxPartsKey):        joinNumberArray(result.MinMax.Parts[:]),
		string(MinMaxkeysKey):         strings.Join(result.MinMax.Keys, "/"),
		string(PartitionGranulesKey):  joinNumberArray(result.Partition.Granules[:]),
		string(PartitionPartsKey):     joinNumberArray(result.Partition.Parts[:]),
		string(PartitionkeysKey):      strings.Join(result.Partition.Keys, "/"),
		string(PrimaryKeyGranulesKey): joinNumberArray(result.PrimaryKey.Granules[:]),
		string(PrimaryKeyPartsKey):    joinNumberArray(result.PrimaryKey.Parts[:]),
		string(PrimaryKeykeysKey):     strings.Join(result.PrimaryKey.Keys, "/"),
	}, nil
}

func (qlb *ExplainBenchmark) OnModuleEnd(results suite.BenchmarkResults) (map[string]string, error) {
	return nil, nil
}

func flatten(nested []interface{}) []interface{} {
	flattened := make([]interface{}, 0)
	for _, i := range nested {
		switch v := i.(type) {
		case []interface{}:
			flattenedSubArray := flatten(v)
			flattened = append(flattened, flattenedSubArray...)
		case interface{}:
			flattened = append(flattened, v)
		default:
		}
	}
	return flattened
}

func joinNumberArray(arr []float64) string {
	var result string
	for i, v := range arr {
		result += fmt.Sprintf("%v", v)
		if i < len(arr)-1 {
			result += "/"
		}
	}
	return result
}
