package benchmarkwriter

import (
	"fmt"

	"github.com/lansweeper/ClickhouseBenchTool/internal/suite"
)

type ConsoleLogWriter struct{}

func NewConsoleLogWriter() *ConsoleLogWriter {
	return &ConsoleLogWriter{}
}

func (clw *ConsoleLogWriter) Write(results suite.SuiteResults) error {

	fmt.Println("IMPRESIÓN DE RESULTADOS EN CONSOLA")
	for moduleName, moduleResults := range results {
		fmt.Printf("Module: %s\n", moduleName)
		for benchmarkName, benchmarkResults := range moduleResults.BenchmarkResultsMap {
			fmt.Printf("  Benchmark: %s\n", benchmarkName)
			for _, result := range benchmarkResults {
				for key, value := range result {
					fmt.Printf("    %s: %s\n", key, value)
				}
			}
		}
	}
	return nil
}
