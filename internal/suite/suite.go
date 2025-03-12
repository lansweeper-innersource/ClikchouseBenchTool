package suite

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/alitto/pond"
	"github.com/lansweeper/ClickhouseBenchTool/internal/benchmark"
	"github.com/lansweeper/ClickhouseBenchTool/internal/datastore"
	"github.com/lansweeper/ClickhouseBenchTool/internal/db"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/viper"
)

type SuiteResults map[string][]map[string]string

type BenchmarkSuiteConfig struct {
	SuitePath         string
	SuiteQueryParams  map[string]interface{}
	NumWorkers        int
	WorkerCapacity    int
	Iterations        int
	ClickhouseCliPath string
	ClickHouseConfig  db.ClickHouseConfig
}

type BenchmarkSuite struct {
	Conn        driver.Conn
	SuiteConfig BenchmarkSuiteConfig
	benchmarks  []benchmark.Benchmark
}

type BenchmarkSuiteOption func(*BenchmarkSuite)

func NewBenchmarkSuite(conn driver.Conn, config BenchmarkSuiteConfig, options ...BenchmarkSuiteOption) *BenchmarkSuite {
	suite := &BenchmarkSuite{
		Conn:        conn,
		benchmarks:  []benchmark.Benchmark{},
		SuiteConfig: config,
	}
	for _, option := range options {
		option(suite)
	}
	return suite
}

func (s *BenchmarkSuite) RunSuite(ctx context.Context) (SuiteResults, error) {
	ds := datastore.CreateDataStore()
	modules, err := ds.GetModules(s.SuiteConfig.SuitePath)
	if err != nil {
		panic(err)
	}

	// Calculate the number of queries to be executed
	numQueries := 0
	for _, module := range modules {
		numQueries += len(module.Queries)
	}

	var bar = progressbar.Default(int64(numQueries))
	pool := pond.New(viper.GetInt("maxWorkers"), viper.GetInt("maxWorkerCapacity"))
	defer pool.StopAndWait()

	var results = SuiteResults{}
	group, poolCtx := pool.GroupContext(ctx)
	for _, module := range modules {
		for _, benchmark := range s.benchmarks {
			for queryIndex, query := range module.Queries {
				results[benchmark.Name()] = make([]map[string]string, len(module.Queries))
				q := query
				group.Submit(func() error {
					params := s.SuiteConfig.SuiteQueryParams
					for k, v := range q.Params {
						params[k] = v
					}
					result, err := benchmark.Run(poolCtx, params, q.Query)
					results[benchmark.Name()][queryIndex] = result
					if err != nil {
						return fmt.Errorf("run benchmark: %w", err)
					}
					bar.Add(1)
					return nil
				})
			}
		}
	}
	bar.RenderBlank()
	err = group.Wait()
	if err != nil {
		return results, fmt.Errorf("run benchmark: %w", err)
	}
	return results, nil
}

func WithBenchmark(benchmark benchmark.Benchmark) func(*BenchmarkSuite) {
	return func(s *BenchmarkSuite) {
		s.benchmarks = append(s.benchmarks, benchmark)
	}
}
