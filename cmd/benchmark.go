package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/lansweeper/ClickhouseBenchTool/internal"
	"github.com/lansweeper/ClickhouseBenchTool/internal/benchmark"
	"github.com/lansweeper/ClickhouseBenchTool/internal/db"
	"github.com/lansweeper/ClickhouseBenchTool/internal/suite"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type tomlParams struct {
	Params map[string]interface{}
}

// benchmarkCmd represents the benchmark command
var benchmarkCmd = &cobra.Command{
	Use:   "benchmark",
	Short: "Runs the benchmark",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
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

		// explainBenchmark := benchmark.NewExplainBenchmark(conn)
		// cliBenchmark := benchmark.NewCliBenchmark(conn, benchmark.CliBenchmarkConfig{
		// 	PathToCli:  cliPath,
		// 	Host:       clickHouseConfig.Host,
		// 	Port:       clickHouseConfig.Port,
		// 	Username:   clickHouseConfig.Username,
		// 	Password:   clickHouseConfig.Password,
		// 	Iterations: viper.GetInt("iterations"),
		// 	Database:   clickHouseConfig.Database,
		// 	Secure:     clickHouseConfig.Secure,
		// })

		benchmarkSuite := suite.NewBenchmarkSuite(conn,
			suite.BenchmarkSuiteConfig{
				SuitePath:         viper.GetString("directory"),
				SuiteQueryParams:  globalParams.Params,
				NumWorkers:        viper.GetInt("maxWorkers"),
				WorkerCapacity:    viper.GetInt("maxWorkerCapacity"),
				Iterations:        viper.GetInt("iterations"),
				ClickhouseCliPath: cliPath,
				ClickHouseConfig:  clickHouseConfig,
			},
			suite.WithBenchmark(benchmark.NewQueryLogBenchmark(conn)),
			suite.WithBenchmark(benchmark.NewQueryResultsBenchmark(conn)),
			// suite.WithBenchmark(explainBenchmark),
			// suite.WithBenchmark(cliBenchmark),
		)

		results, err := benchmarkSuite.RunSuite(cmd.Context())
		fmt.Println(results)
		resultsJson, err := json.Marshal(results)
		fmt.Println(string(resultsJson))
		if err != nil {
			panic(err)
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
	rootCmd.PersistentFlags().IntP("iterations", "i", 1, "Number of iterations to run the query")

	viper.BindPFlag("database.port", rootCmd.PersistentFlags().Lookup("port"))
	viper.BindPFlag("database.host", rootCmd.PersistentFlags().Lookup("host"))
	viper.BindPFlag("database.database", rootCmd.PersistentFlags().Lookup("db"))
	viper.BindPFlag("database.user", rootCmd.PersistentFlags().Lookup("user"))
	viper.BindPFlag("database.password", rootCmd.PersistentFlags().Lookup("password"))
	viper.BindPFlag("directory", rootCmd.PersistentFlags().Lookup("directory"))
	viper.BindPFlag("maxWorkers", rootCmd.PersistentFlags().Lookup("maxWorkers"))
	viper.BindPFlag("maxWorkerCapacity", rootCmd.PersistentFlags().Lookup("maxWorkerCapacity"))
	viper.BindPFlag("iterations", rootCmd.PersistentFlags().Lookup("iterations"))
}
