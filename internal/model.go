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

type BenchmarkResults struct {
	ModuleName string
	QueryName  string
}
