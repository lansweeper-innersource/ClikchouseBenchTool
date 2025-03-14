package suite

import (
	"context"
)

type Benchmark interface {
	Run(ctx context.Context, queryParams map[string]any, query string) (map[string]string, error)
	Name() string
	OnModuleEnd(results BenchmarkResults) (map[string]string, error)
}
