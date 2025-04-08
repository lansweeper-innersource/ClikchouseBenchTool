package benchmarkwriter

import (
	"fmt"
	"os"

	"github.com/lansweeper/ClickhouseBenchTool/internal/benchmark"
	"github.com/lansweeper/ClickhouseBenchTool/internal/suite"
	md "github.com/nao1215/markdown"
)

type MdLogWriter struct{}

func NewMdLogWriter() *MdLogWriter {
	return &MdLogWriter{}
}

func queryLogBenchmarkResultsToTableRows(queryLogBenchmark suite.BenchmarkResults, doc *md.Markdown) [][]string {

	var rows [][]string
	for _, row := range queryLogBenchmark {
		rows = append(rows, []string{

			row[string(benchmark.LogQueryNameKey)],
			row[string(benchmark.MemoryUsageReadableKey)],
			row[string(benchmark.OSCPUVirtualTimeReadableKey)],
			row[string(benchmark.ReadBytesReadableKey)],
			row[string(benchmark.ReadRowsKey)],
			row[string(benchmark.ResultBytesReadableKey)],
			row[string(benchmark.ResultRowsKey)],
		})
	}

	doc.H2("Query Log Benchmark Results")
	doc.Table(md.TableSet{
		Header: []string{
			"Query name",
			"memory_usage_readable",
			"os_cpu_virtual_time_readable",
			"read_bytes_readable",
			"read_rows",
			"result_bytes_readable",
			"result_rows"},
		Rows: rows,
	})

	return nil
}

func explainBenchmarkResultsToTableRows(explainBenchmark suite.BenchmarkResults, doc *md.Markdown) [][]string {

	var rows [][]string
	for _, row := range explainBenchmark {
		rows = append(rows, []string{
			row[string(benchmark.ExplainQueryKey)],
			row[string(benchmark.MinMaxGranulesKey)],
			row[string(benchmark.MinMaxPartsKey)],
			row[string(benchmark.MinMaxkeysKey)],
			row[string(benchmark.PartitionGranulesKey)],
			row[string(benchmark.PartitionPartsKey)],
			row[string(benchmark.PartitionkeysKey)],
			row[string(benchmark.PrimaryKeyGranulesKey)],
			row[string(benchmark.PrimaryKeyPartsKey)],
			row[string(benchmark.PrimaryKeykeysKey)],
		})
	}

	doc.H2("Explain Benchmark Results")

	doc.Table(md.TableSet{
		Header: []string{
			"Query name",
			"minMaxGranules",
			"minMaxParts",
			"minMaxkeys",
			"partitionGranules",
			"partitionParts",
			"partitionkeys",
			"primaryKeyGranules",
			"primaryKeyParts",
			"primaryKeykeys"},
		Rows: rows,
	})

	return nil
}

func cliBenchmarkResultsToTableRows(cliBenchmark suite.BenchmarkResults, doc *md.Markdown) [][]string {

	var rows [][]string
	for _, row := range cliBenchmark {
		rows = append(rows, []string{
			row[string(benchmark.CliQueryNameKey)],
			row[string(benchmark.ResultMiBsKey)],
			row[string(benchmark.ExecutionsKey)],
			row[string(benchmark.QPSKey)],
			row[string(benchmark.ResultRpsKey)],
			row[string(benchmark.MiBsKey)],
			row[string(benchmark.RPSKeys)],
		})
	}

	doc.H2("CLI Benchmark Results")
	doc.Table(md.TableSet{
		Header: []string{
			"Query name",
			"result_miBs",
			"executions",
			"QPS",
			"result_rps",
			"MiBs",
			"RPS"},
		Rows: rows,
	})

	return nil
}

func (mlw *MdLogWriter) Write(results suite.SuiteResults) error {

	file, err := os.Create("./benchmarkResults.md")
	if err != nil {
		return err
	}
	defer file.Close()
	doc := md.NewMarkdown(file)
	doc.H1("Benchmark Results")
	doc.H2("Metrics legend")
	doc.H3("Explain Benchmark Results")
	doc.BulletList(
		md.Bold("Min Max Granules:")+" Minimum and maximum number of granules in the query plan.",
		md.Bold("Min Max Parts:")+" Minimum and maximum number of parts in the query plan.",
		md.Bold("Min Max Keys:")+" Minimum and maximum number of keys in the query plan.",
		md.Bold("Partition Granules:")+" Number of granules in the partition.",
		md.Bold("Partition Parts:")+" Number of parts in the partition.",
		md.Bold("Partition Keys:")+" Number of keys in the partition.",
		md.Bold("Primary Key Granules:")+" Number of granules in the primary key.",
		md.Bold("Primary Key Parts:")+" Number of parts in the primary key.",
		md.Bold("Primary Key Keys:")+" Number of keys in the primary key.")
	doc.H3("CLI Benchmark Results")
	doc.BulletList(
		md.Bold("Result MiB/s:")+" Speed of the result in MiB/s.",
		md.Bold("Executions:")+" Number of executions of the query.",
		md.Bold("QPS:")+" Queries per second.",
		md.Bold("Result RPS:")+" Result rows per second.",
		md.Bold("MiB/s:")+" Speed of the query in MiB/s.",
		md.Bold("RPS:")+" Rows per second.")
	doc.H3("Query Log Benchmark Results")
	doc.BulletList(
		md.Bold("Memory Usage:")+" Total memory consumed during the execution of the query.",
		md.Bold("Memory Usage (Readable):")+" Human-readable format of the memory consumed (e.g., MiB)",
		md.Bold("OS CPU Virtual Time:")+" Total CPU time spent by the operating system to execute the query, measured in microseconds.",
		md.Bold("OS CPU Virtual Time (Readable):")+" Human-readable format of the CPU time (e.g., seconds, milliseconds, microseconds).",
		md.Bold("Read Bytes:")+" Total amount of data read by the query from the database, measured in bytes.",
		md.Bold("Read Bytes (Readable):")+" Human-readable format of the data read (e.g., KiB, MiB).",
		md.Bold("Read Rows:")+" Total number of rows read by the query from the database.",
		md.Bold("Result Bytes:")+" Total size of the query result, measured in bytes.",
		md.Bold("Result Bytes (Readable):")+" Human-readable format of the query result size (e.g., KiB, MiB).",
		md.Bold("Result Rows:")+" Total number of rows returned as the result of the query.")

	testIndex := 0
	for testName, testResults := range results {

		doc.H1(fmt.Sprintf("%s Results", testName))

		for benchmarkName, benchmark := range testResults.BenchmarkResultsMap {

			switch benchmarkName {
			case "queryLogBenchmark":
				queryLogBenchmarkResultsToTableRows(benchmark, doc)

				valuesPerQueryFromMemory := ObtainValuesPerQuery("memory_usage", benchmark)
				memoryPlotParams := CreatePlotParams{
					ValuePerQuery: valuesPerQueryFromMemory,
					TestName:      testName,
					Doc:           doc,
					Index:         testIndex,
					File:          "memory_usage",
					PTitleText:    "Memory Usage Comparison",
					PXLabelText:   "Test Cases",
					PYLabelText:   "Memory (MiB)",
				}
				memoryFilePath, err := PlotCreator(memoryPlotParams)
				if err != nil {
					return fmt.Errorf("error creating memory plot: %w", err)
				}
				// Add the memory plot to the markdown document

				doc.PlainTextf(md.Image(testName, memoryFilePath))

				valuesPerQueryFromCpu := ObtainValuesPerQuery("os_cpu_virtual_time", benchmark)
				cpuPlotParams := CreatePlotParams{
					ValuePerQuery: valuesPerQueryFromCpu,
					TestName:      testName,
					Doc:           doc,
					Index:         testIndex,
					File:          "os_cpu_virtual_time",
					PTitleText:    "Cpu time comparison",
					PXLabelText:   "Queries",
					PYLabelText:   "Microseconds",
				}
				cpuFilePath, err := PlotCreator(cpuPlotParams)
				if err != nil {
					return fmt.Errorf("error creating memory plot: %w", err)
				}

				doc.PlainTextf(md.Image(testName, cpuFilePath))

			case "explainBenchmark":
				explainBenchmarkResultsToTableRows(benchmark, doc)
			case "cliBenchmark":
				cliBenchmarkResultsToTableRows(benchmark, doc)
				valuesPerQueryFromQPS := ObtainValuesPerQuery("QPS", benchmark)
				qpsPlotParams := CreatePlotParams{
					ValuePerQuery: valuesPerQueryFromQPS,
					TestName:      testName,
					Doc:           doc,
					Index:         testIndex,
					File:          "QPS",
					PTitleText:    "Queries per second comparison",
					PXLabelText:   "Queries",
					PYLabelText:   "Quries Per Second",
				}
				qpsFilePath, err := PlotCreator(qpsPlotParams)
				if err != nil {
					return fmt.Errorf("error creating QPS plot: %w", err)
				}
				doc.PlainTextf(md.Image(testName, qpsFilePath))

				valuesPerQueryFromMIBS := ObtainValuesPerQuery("QPS", benchmark)
				mibsPlotParams := CreatePlotParams{
					ValuePerQuery: valuesPerQueryFromMIBS,
					TestName:      testName,
					Doc:           doc,
					Index:         testIndex,
					File:          "QPS",
					PTitleText:    "Queries per second comparison",
					PXLabelText:   "Queries",
					PYLabelText:   "Quries Per Second",
				}
				mibsFilePath, err := PlotCreator(mibsPlotParams)
				if err != nil {
					return fmt.Errorf("error creating MIBS plot: %w", err)
				}
				doc.PlainTextf(md.Image(testName, mibsFilePath))

			default:
				fmt.Println("Unknown Benchmark")
			}
		}
		testIndex++

	}

	err = doc.Build()
	if err != nil {
		return fmt.Errorf("build markdown: %w", err)
	}

	return nil
}
