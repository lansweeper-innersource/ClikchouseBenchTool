package internal

import (
	"fmt"

	"github.com/spf13/viper"
)

type ConfigOptions struct {
	FileName string
	FileType string
}

func LoadConfig(options ConfigOptions) {
	viper.SetConfigName(options.FileName)
	viper.SetConfigType(options.FileType)

	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("No config file found")
	}
}
