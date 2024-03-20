package benchmark

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/lansweeper/ClickhouseBenchTool/internal"
	"github.com/lansweeper/ClickhouseBenchTool/internal/db"
)

type CliBenchmarkConfig struct {
	Iterations int
	Query      string
}

func RunCliBenchmark(pathToCli string, chConfig db.ClickHouseConfig, benchConfig CliBenchmarkConfig) (internal.CliBenchmarkResults, error) {
	cliResults := internal.CliBenchmarkResults{}
	args := []string{
		"benchmark",
		fmt.Sprintf("--host=%s", chConfig.Host),
		fmt.Sprintf("--port=%d", chConfig.Port),
		fmt.Sprintf("--user=%s", chConfig.Username),
		fmt.Sprintf("--password=%s", chConfig.Password),
		fmt.Sprintf("--iterations=%d", benchConfig.Iterations),
		fmt.Sprintf("--database=%s", chConfig.Database),
		fmt.Sprintf("--query=%s", benchConfig.Query),
	}

	if chConfig.Secure {
		args = append(args, "--secure")
	}

	runBenchmarkCommand := exec.Command(pathToCli, args...)
	output, err := runBenchmarkCommand.CombinedOutput()
	if err != nil {
		fmt.Println(string(output))
		return cliResults, fmt.Errorf("execute benchmark cli command: %w", err)
	}
	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, chConfig.Host) {
			lineWithoutFinalDot := strings.TrimSuffix(line, ".")
			lineResults := strings.Split(lineWithoutFinalDot, ", ")
			for _, result := range lineResults {
				resultElement := strings.Split(result, ": ")
				switch resultElement[0] {
				case "queries":
					numericValue, err := strconv.Atoi(resultElement[1])
					if err != nil {
						return cliResults, fmt.Errorf("parse queries: %w", err)
					}
					cliResults.Executions = numericValue
				case "QPS":
					numericValue, err := strconv.ParseFloat(resultElement[1], 64)
					if err != nil {
						return cliResults, fmt.Errorf("parse QPS: %w", err)
					}
					cliResults.QPS = numericValue
				case "MiB/s":
					numericValue, err := strconv.ParseFloat(resultElement[1], 64)
					if err != nil {
						return cliResults, fmt.Errorf("parse MiB/s: %w", err)
					}
					cliResults.MiBs = numericValue
				case "RPS":
					numericValue, err := strconv.ParseFloat(resultElement[1], 64)
					if err != nil {
						return cliResults, fmt.Errorf("parse RPS: %w", err)
					}
					cliResults.RPS = numericValue
				case "result MiB/s":
					numericValue, err := strconv.ParseFloat(resultElement[1], 64)
					if err != nil {
						return cliResults, fmt.Errorf("parse result MiB/s: %w", err)
					}
					cliResults.ResultMiBs = numericValue
				case "result RPS":
					numericValue, err := strconv.ParseFloat(resultElement[1], 64)
					if err != nil {
						return cliResults, fmt.Errorf("parse result RPS: %w", err)
					}
					cliResults.ResultRps = numericValue
				}

			}
		}
	}
	return cliResults, nil
}
