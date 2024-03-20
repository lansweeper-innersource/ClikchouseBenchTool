package benchmark

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/lansweeper/ClickhouseBenchTool/internal"
	"github.com/lansweeper/ClickhouseBenchTool/internal/db"
)

type BenchmarkList struct {
	EnableCliBenchmark     bool
	EnableExplainBenchmark bool
}

type BenchmarkConfig struct {
	Benchmarks       *BenchmarkList
	Conn             driver.Conn
	CliPath          string
	ClickHouseConfig db.ClickHouseConfig
	Query            string
	Params           map[string]interface{}
	Iterations       int
}

func RunBenchmark(ctx context.Context, benchmarkConfig BenchmarkConfig) (internal.BenchmarkResults, error) {
	benchmarkList := BenchmarkList{
		EnableCliBenchmark:     true,
		EnableExplainBenchmark: true,
	}
	if benchmarkConfig.Benchmarks != nil {
		benchmarkList = *benchmarkConfig.Benchmarks
	}

	benchmarkResults := internal.BenchmarkResults{}
	queryLogResults, err := RunQueryLogBenchmark(ctx, benchmarkConfig.Conn, benchmarkConfig.Query, benchmarkConfig.Params)
	if err != nil {
		return benchmarkResults, fmt.Errorf("run Query log benchmark: %w", err)
	}
	benchmarkResults.QueryLogBenchmarkResults = &queryLogResults

	if benchmarkList.EnableCliBenchmark {
		explainResults, err := ExplainBenchmark(ctx, benchmarkConfig.Conn, queryLogResults.Query)
		if err != nil {
			return benchmarkResults, fmt.Errorf("run explain benchmark: %w", err)
		}
		benchmarkResults.QueryExplainBenchmarkResult = &explainResults
	}

	if benchmarkList.EnableCliBenchmark {
		if benchmarkConfig.Iterations == 0 {
			benchmarkConfig.Iterations = 1
		}
		cliResults, err := RunCliBenchmark(benchmarkConfig.CliPath, benchmarkConfig.ClickHouseConfig,
			CliBenchmarkConfig{
				Query:      queryLogResults.Query,
				Iterations: benchmarkConfig.Iterations,
			})
		if err != nil {
			fmt.Println(err)
			return benchmarkResults, fmt.Errorf("run cli benchmark: %w", err)
		}
		benchmarkResults.CliBenchmarkResults = &cliResults
	}
	return benchmarkResults, nil
}
