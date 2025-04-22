package benchmarkwriter

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/lansweeper/ClickhouseBenchTool/internal/suite"
)

type JsonLogWriter struct{}

func NewJsonLogWriter() *JsonLogWriter {
	return &JsonLogWriter{}
}

func (jlw *JsonLogWriter) Write(results suite.SuiteResults) error {

	jsonFile, err := os.Create("./benchmarkResults.json")
	if err != nil {
		return fmt.Errorf("creating params file benchmarkResults.json: %w", err)
	}
	defer jsonFile.Close()
	paramsFileContent, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Errorf("marshalling params for file benchmarkResults.json: %w", err)
	}
	_, err = jsonFile.Write(paramsFileContent)
	if err != nil {
		return fmt.Errorf("writing params file benchmarkResults.json: %w", err)
	}
	return nil
}
