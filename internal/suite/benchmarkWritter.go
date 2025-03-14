package suite

type BenchmarkWritter interface {
	Write(results SuiteResults) error
}
