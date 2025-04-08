package suite

type BenchmarkWriter interface {
	Write(results SuiteResults) error
}
