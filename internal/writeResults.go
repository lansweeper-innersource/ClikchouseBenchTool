package internal

import (
	"fmt"
	"os"

	md "github.com/nao1215/markdown"
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

	for moduleName, _ := range resultsByModule {
		doc.H2(fmt.Sprintf("%s Results", moduleName))
		queryLogRows := [][]string{}
		cliRows := [][]string{}
		explainRows := [][]string{}

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
