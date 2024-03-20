package cmd

import (
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/alitto/pond"
	"github.com/lansweeper/ClickhouseBenchTool/internal"
	"github.com/lansweeper/ClickhouseBenchTool/internal/benchmark"
	"github.com/lansweeper/ClickhouseBenchTool/internal/datastore"
	"github.com/lansweeper/ClickhouseBenchTool/internal/db"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/schollz/progressbar/v3"
)

type tomlParams struct {
	Params map[string]interface{}
}

// benchmarkCmd represents the benchmark command
var benchmarkCmd = &cobra.Command{
	Use:   "benchmark",
	Short: "",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		ds := datastore.CreateDataStore()
		cliPath, err := internal.CheckOrInstallCHCli()
		if err != nil {
			panic(err)
		}

		// Read params from toml file because viper lowercases map keys
		// https://github.com/spf13/viper/issues/373
		tomlFile, err := os.ReadFile("config.toml")
		if err != nil {
			panic(err)
		}
		globalParams := tomlParams{}
		_, err = toml.Decode(string(tomlFile), &globalParams)
		if err != nil {
			panic(err)
		}

		// Connect ClickHouse client
		clickHouseConfig := db.ClickHouseConfig{
			Port:     viper.GetInt("database.port"),
			Host:     viper.GetString("database.host"),
			Database: viper.GetString("database.database"),
			Username: viper.GetString("database.user"),
			Password: viper.GetString("database.password"),
			Secure:   viper.GetBool("database.secure"),
		}
		conn, err := db.GetClickHouse(cmd.Context(), clickHouseConfig)
		if err != nil {
			panic(err)
		}

		modules, err := ds.GetModules(viper.GetString("directory"))
		if err != nil {
			panic(err)
		}

		// Calculate the number of queries to be executed
		numQueries := 0
		for _, module := range modules {
			numQueries += len(module.Queries)
		}
		resultsChan := make(chan internal.BenchmarkResults, numQueries)
		var bar = progressbar.Default(int64(numQueries))

		pool := pond.New(viper.GetInt("maxWorkers"), viper.GetInt("maxWorkerCapacity"))
		defer pool.StopAndWait()

		group, poolCtx := pool.GroupContext(cmd.Context())
		for _, module := range modules {
			for _, query := range module.Queries {
				q := query
				group.Submit(func() error {
					params := globalParams.Params
					for k, v := range q.Params {
						params[k] = v
					}
					benchmarkResults, err := benchmark.RunBenchmark(poolCtx, benchmark.BenchmarkConfig{
						Conn:             conn,
						CliPath:          cliPath,
						ClickHouseConfig: clickHouseConfig,
						Query:            q.Query,
						Params:           params,
					})
					if err != nil {
						return err
					}
					benchmarkResults.ModuleName = module.Name
					benchmarkResults.QueryName = q.Name
					resultsChan <- benchmarkResults
					bar.Add(1)
					return nil
				})
			}
		}
		// bar.RenderBlank()
		err = group.Wait()
		if err != nil {
			panic(err)
		}

		results := []internal.BenchmarkResults{}
		for i := 0; i < numQueries; i++ {
			r := <-resultsChan
			results = append(results, r)
		}
		if err != nil {
			fmt.Printf("Failed to run benchmark: %v", err)
		}
		err = internal.WriteResults(results, "results.md")
		if err != nil {
			fmt.Printf("Failed to write results: %v", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(benchmarkCmd)

	cobra.OnInitialize(func() {
		internal.LoadConfig(internal.ConfigOptions{
			FileName: "config",
			FileType: "toml",
		})
	})

	rootCmd.PersistentFlags().String("host", "localhost", "ClickHouse host")
	rootCmd.PersistentFlags().IntP("port", "p", 9440, "ClickHouse port")
	rootCmd.PersistentFlags().String("db", "", "ClickHouse database")
	rootCmd.PersistentFlags().StringP("user", "u", "", "ClickHouse user")
	rootCmd.PersistentFlags().StringP("password", "w", "", "ClickHouse password")
	rootCmd.PersistentFlags().StringP("directory", "d", "queries", "Directory containing the modules to be executed")
	rootCmd.PersistentFlags().IntP("maxWorkers", "", 1, "Number of workers to run the queries")
	rootCmd.PersistentFlags().IntP("maxWorkerCapacity", "", 1, "Max capacity for each worker")

	viper.BindPFlag("database.port", rootCmd.PersistentFlags().Lookup("port"))
	viper.BindPFlag("database.host", rootCmd.PersistentFlags().Lookup("host"))
	viper.BindPFlag("database.database", rootCmd.PersistentFlags().Lookup("db"))
	viper.BindPFlag("database.user", rootCmd.PersistentFlags().Lookup("user"))
	viper.BindPFlag("database.password", rootCmd.PersistentFlags().Lookup("password"))
	viper.BindPFlag("directory", rootCmd.PersistentFlags().Lookup("directory"))
	viper.BindPFlag("maxWorkers", rootCmd.PersistentFlags().Lookup("maxWorkers"))
	viper.BindPFlag("maxWorkerCapacity", rootCmd.PersistentFlags().Lookup("maxWorkerCapacity"))
}
