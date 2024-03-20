package internal

// Module model
type Module struct {
	Executed bool
	Name     string
	Queries  []ModuleQuery
}

type ModuleQuery struct {
	Name         string
	Query        string
	GlobalParams map[string]any
	Params       map[string]any
	Executed     bool
}

// benchmark models
type CliBenchmarkResults struct {
	ResultMiBs float64
	Executions int
	QPS        float64
	ResultRps  float64
	MiBs       float64
	RPS        float64
}

type QueryExplainResultData struct {
	Type     string
	Keys     []string
	Parts    [2]float64
	Granules [2]float64
}

type QueryExplainBenchmarkResult struct {
	MinMax     QueryExplainResultData
	Partition  QueryExplainResultData
	PrimaryKey QueryExplainResultData
}

type QueryLogBenchmarkResults struct {
	ReadBytes                    uint64 `ch:"read_bytes"`
	ResultBytes                  uint64 `ch:"result_bytes"`
	ReadRows                     uint64 `ch:"read_rows"`
	ResultRows                   uint64 `ch:"result_rows"`
	MemoryUsage                  uint64 `ch:"memory_usage"`
	QueryId                      string `ch:"query_id"`
	Query                        string `ch:"query"`
	OSCPUVirtualTimeMicroseconds uint64 `ch:"os_cpu_virtual_time_microseconds"`
}

type BenchmarkResults struct {
	ModuleName string
	QueryName  string
	*CliBenchmarkResults
	*QueryExplainBenchmarkResult
	*QueryLogBenchmarkResults
}
