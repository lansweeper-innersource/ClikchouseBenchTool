package benchmarkwriter

import (
	"fmt"
	"os"
	"strconv"

	"github.com/lansweeper/ClickhouseBenchTool/internal/suite"
	md "github.com/nao1215/markdown"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

type CreatePlotParams struct {
	ValuePerQuery map[string]float64
	TestName      string
	Doc           *md.Markdown
	Index         int
	File          string
	PTitleText    string
	PXLabelText   string
	PYLabelText   string
	QueryNames    []string
}

func PlotCreator(params CreatePlotParams) (string, error) {
	newPlot := plot.New()

	newPlot.Title.Text = params.PTitleText
	newPlot.X.Label.Text = params.PXLabelText
	newPlot.Y.Label.Text = params.PYLabelText
	newPlot.Legend.Top = true

	barWidth := vg.Points(30)
	index := 0
	for queryName, value := range params.ValuePerQuery {
		plotterValue := plotter.Values{value}

		bar, err := plotter.NewBarChart(plotterValue, barWidth)
		if err != nil {
			panic(err)
		}
		bar.LineStyle.Width = vg.Length(0)
		bar.Color = plotutil.Color(index)
		bar.Offset = (barWidth * vg.Length(index)) - (vg.Length(len(params.ValuePerQuery))*barWidth)/2

		newPlot.Add(bar)
		newPlot.Legend.Add(queryName, bar)
		index++
	}

	newPlot.NominalX(params.TestName)

	currentFileName := params.TestName + "_" + params.File
	directoryPath := "./plots"
	currentFilepath := directoryPath + "/" + currentFileName + ".png"

	if err := os.MkdirAll(directoryPath, 0755); err != nil {
		return "", fmt.Errorf("error creating plots directory: %w", err)
	}

	if err := newPlot.Save(5*vg.Inch, 3*vg.Inch, currentFilepath); err != nil {
		fmt.Println("Error saving plot for", currentFileName, ":", err)

	}

	return currentFilepath, nil
}

func ObtainValuesPerQuery(
	field string,
	benchmark suite.BenchmarkResults,
) map[string]float64 {
	valuesPerQuery := make(map[string]float64)
	for i, query := range benchmark {

		queryName, ok := query["query_name"]
		if !ok {
			fmt.Printf("Error: No query_name found in benchmark result at index %d\n", i)
			queryName = fmt.Sprintf("Unknown_Query_%d", i)
		}

		fieldValue, ok := query[field]
		if !ok {
			fmt.Printf("Error: Field '%s' not found for query '%s'\n", field, queryName)
			continue
		}

		value, err := strconv.ParseFloat(fieldValue, 64)
		if err != nil {
			fmt.Printf("Error converting %s to float64: %v\n", fieldValue, err)
			continue
		}

		valuesPerQuery[queryName] = value

	}
	return valuesPerQuery
}
