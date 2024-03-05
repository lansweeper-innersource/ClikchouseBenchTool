package internal

import (
	"fmt"
	"os"
	"strings"

	md "github.com/go-spectest/markdown"
)

func joinNumberArray(arr []float64) string {
	var result string
	for i, v := range arr {
		result += fmt.Sprintf("%v", v)
		if i < len(arr)-1 {
			result += "/"
		}
	}
	return result
}

func WriteResults(results []BenchmarkResults, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	doc := md.NewMarkdown(file)
	doc.H1("Benchmark Results")
	doc.H2("Metrics legend")
	doc.BulletList(
		md.Bold("Query:")+" Query name",
		md.Bold("Bytes read:")+" Amount of bytes that ClickHouse has loaded to execute the query",
		md.Bold("Rows read:")+" Number of rows read to execute the query.",
		md.Bold("QPS:")+" Queries executed per second",
		md.Bold("Result RPS:")+" How many megabytes the server reads per second",
		md.Bold("Result MiB/s:")+" How many rows placed by the server to the result of a query per second",
		md.Bold("RPS:")+" How many rows the server reads per second")

	doc.H2("Results")
	queryLogRows := [][]string{}
	cliRows := [][]string{}
	explainRows := [][]string{}
	for _, result := range results {
		queryLogRows = append(queryLogRows, []string{
			result.QueryId,
			fmt.Sprintf("%d", result.QueryLogBenchmarkResults.MemoryUsage),
			fmt.Sprintf("%d", result.QueryLogBenchmarkResults.ReadRows),
			fmt.Sprintf("%d", result.QueryLogBenchmarkResults.ReadBytes),
			fmt.Sprintf("%d", result.QueryLogBenchmarkResults.ResultBytes),
			fmt.Sprintf("%d", result.QueryLogBenchmarkResults.ResultRows),
			fmt.Sprintf("%d", result.QueryLogBenchmarkResults.OSCPUVirtualTimeMicroseconds),
		})

		cliRows = append(cliRows, []string{
			result.QueryId,
			fmt.Sprintf("%f", result.CliBenchmarkResults.QPS),
			fmt.Sprintf("%f", result.CliBenchmarkResults.RPS),
			fmt.Sprintf("%d", result.CliBenchmarkResults.Executions),
			fmt.Sprintf("%f", result.CliBenchmarkResults.ResultMiBs),
			fmt.Sprintf("%f", result.CliBenchmarkResults.ResultRps),
		})

		explainRows = append(explainRows, []string{
			result.QueryId,
			joinNumberArray(result.QueryExplainBenchmarkResult.MinMax.Granules[:]),
			joinNumberArray(result.QueryExplainBenchmarkResult.MinMax.Parts[:]),
			strings.Join(result.QueryExplainBenchmarkResult.MinMax.Keys, "/"),
			joinNumberArray(result.QueryExplainBenchmarkResult.Partition.Granules[:]),
			joinNumberArray(result.QueryExplainBenchmarkResult.Partition.Parts[:]),
			strings.Join(result.QueryExplainBenchmarkResult.Partition.Keys, "/"),
			joinNumberArray(result.QueryExplainBenchmarkResult.PrimaryKey.Granules[:]),
			joinNumberArray(result.QueryExplainBenchmarkResult.PrimaryKey.Parts[:]),
			strings.Join(result.QueryExplainBenchmarkResult.PrimaryKey.Keys, "/"),
		})

	}
	doc.Table(md.TableSet{
		Header: []string{"Query", "MemoryUsage", "ReadRows", "ReadBytes", "ResultBytes", "ResultRows", "CPU"},
		Rows:   queryLogRows,
	})
	doc.Table(md.TableSet{
		Header: []string{"Query", "QPS", "RPS", "Executions", "ResultMiBs", "ResultRps"},
		Rows:   cliRows,
	})
	doc.Table(md.TableSet{
		Header: []string{"Query", "MinMax Granules", "MinMax Parts", "MinMax keys", "Partition Granules", "partition Parts", "Partition keys", "PrimaryKey Granules", "PrimaryKey Parts", "PrimaryKey keys"},
		Rows:   explainRows,
	})

	err = doc.Build()
	if err != nil {
		return fmt.Errorf("build markdown: %w", err)
	}
	return nil
}
