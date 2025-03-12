package benchmark

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type cliBenchmarkResults struct {
	ResultMiBs float64
	Executions int
	QPS        float64
	ResultRps  float64
	MiBs       float64
	RPS        float64
}

const CliBenchmarkName = "cliBenchmark"

type CliBenchmarkConfig struct {
	PathToCli  string
	Host       string
	Port       int
	Username   string
	Password   string
	Iterations int
	Database   string
	Secure     bool
}

type CliBenchmark struct {
	conn   clickhouse.Conn
	config CliBenchmarkConfig
}

func NewCliBenchmark(conn clickhouse.Conn, config CliBenchmarkConfig) *CliBenchmark {
	return &CliBenchmark{
		conn:   conn,
		config: config,
	}
}

func (qlb *CliBenchmark) Name() string {
	return CliBenchmarkName
}

func (qlb *CliBenchmark) Run(ctx context.Context, queryParams map[string]any, query string) (map[string]string, error) {
	cliResults := cliBenchmarkResults{}
	benchConfig := qlb.config
	args := []string{
		"benchmark",
		fmt.Sprintf("--host=%s", benchConfig.Host),
		fmt.Sprintf("--port=%d", benchConfig.Port),
		fmt.Sprintf("--user=%s", benchConfig.Username),
		fmt.Sprintf("--password=%s", benchConfig.Password),
		fmt.Sprintf("--iterations=%d", benchConfig.Iterations),
		fmt.Sprintf("--database=%s", benchConfig.Database),
		fmt.Sprintf("--query=%s", query),
	}

	if benchConfig.Secure {
		args = append(args, "--secure")
	}

	runBenchmarkCommand := exec.Command(benchConfig.PathToCli, args...)
	output, err := runBenchmarkCommand.CombinedOutput()
	if err != nil {
		fmt.Println(string(output))
		return map[string]string{}, fmt.Errorf("execute benchmark cli command: %w", err)
	}
	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, benchConfig.Host) {
			lineWithoutFinalDot := strings.TrimSuffix(line, ".")
			lineResults := strings.Split(lineWithoutFinalDot, ", ")
			for _, result := range lineResults {
				resultElement := strings.Split(result, ": ")
				switch resultElement[0] {
				case "queries":
					numericValue, err := strconv.Atoi(resultElement[1])
					if err != nil {
						return map[string]string{}, fmt.Errorf("parse queries: %w", err)
					}
					cliResults.Executions = numericValue
				case "QPS":
					numericValue, err := strconv.ParseFloat(resultElement[1], 64)
					if err != nil {
						return map[string]string{}, fmt.Errorf("parse QPS: %w", err)
					}
					cliResults.QPS = numericValue
				case "MiB/s":
					numericValue, err := strconv.ParseFloat(resultElement[1], 64)
					if err != nil {
						return map[string]string{}, fmt.Errorf("parse MiB/s: %w", err)
					}
					cliResults.MiBs = numericValue
				case "RPS":
					numericValue, err := strconv.ParseFloat(resultElement[1], 64)
					if err != nil {
						return map[string]string{}, fmt.Errorf("parse RPS: %w", err)
					}
					cliResults.RPS = numericValue
				case "result MiB/s":
					numericValue, err := strconv.ParseFloat(resultElement[1], 64)
					if err != nil {
						return map[string]string{}, fmt.Errorf("parse result MiB/s: %w", err)
					}
					cliResults.ResultMiBs = numericValue
				case "result RPS":
					numericValue, err := strconv.ParseFloat(resultElement[1], 64)
					if err != nil {
						return map[string]string{}, fmt.Errorf("parse result RPS: %w", err)
					}
					cliResults.ResultRps = numericValue
				}

			}
		}
	}
	return map[string]string{
		"resultMiBs": fmt.Sprintf("%f", cliResults.ResultMiBs),
		"executions": fmt.Sprintf("%v", cliResults.Executions),
		"QPS":        fmt.Sprintf("%f", cliResults.QPS),
		"resultRps":  fmt.Sprintf("%f", cliResults.ResultRps),
		"MiBs":       fmt.Sprintf("%f", cliResults.MiBs),
		"RPS":        fmt.Sprintf("%f", cliResults.RPS),
	}, nil
}
