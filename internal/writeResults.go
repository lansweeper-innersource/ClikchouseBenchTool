package internal

import (
	"fmt"
	"os"
	"strings"

	md "github.com/go-spectest/markdown"
)

func bytesToMiB(bytes uint64) float64 {
	return float64(bytes) / 1024 / 1024
}

func round(num float64) float64 {
	return float64(int(num*100)) / 100
}

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
	resultsByModule := map[string][]BenchmarkResults{}
	for _, result := range results {
		resultsByModule[result.ModuleName] = append(resultsByModule[result.ModuleName], result)
	}
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

	for moduleName, moduleResults := range resultsByModule {
		doc.H2(fmt.Sprintf("%s Results", moduleName))
		queryLogRows := [][]string{}
		cliRows := [][]string{}
		explainRows := [][]string{}
		for _, result := range moduleResults {
			queryLogRows = append(queryLogRows, []string{
				result.QueryName,
				fmt.Sprintf("%v", round(bytesToMiB(result.QueryLogBenchmarkResults.MemoryUsage))),
				fmt.Sprintf("%v", result.QueryLogBenchmarkResults.ReadRows),
				fmt.Sprintf("%v", round(bytesToMiB(result.QueryLogBenchmarkResults.ReadBytes))),
				fmt.Sprintf("%v", round(bytesToMiB(result.QueryLogBenchmarkResults.ResultBytes))),
				fmt.Sprintf("%v", result.QueryLogBenchmarkResults.ResultRows),
				fmt.Sprintf("%v", result.QueryLogBenchmarkResults.OSCPUVirtualTimeMicroseconds),
			})

			cliRows = append(cliRows, []string{
				result.QueryName,
				fmt.Sprintf("%f", result.CliBenchmarkResults.QPS),
				fmt.Sprintf("%f", result.CliBenchmarkResults.RPS),
				fmt.Sprintf("%f", result.CliBenchmarkResults.ResultMiBs),
				fmt.Sprintf("%f", result.CliBenchmarkResults.ResultRps),
				fmt.Sprintf("%v", result.CliBenchmarkResults.Executions),
			})

			explainRows = append(explainRows, []string{
				result.QueryName,
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
			Header: []string{"Query", "MemoryUsage", "ReadRows", "ReadMB", "ResultMB", "ResultRows", "CPU"},
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
	}

	err = doc.Build()
	if err != nil {
		return fmt.Errorf("build markdown: %w", err)
	}
	return nil
}
