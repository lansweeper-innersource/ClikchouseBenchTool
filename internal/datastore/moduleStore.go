package datastore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/lansweeper/ClickhouseBenchTool/internal"
)

func (d *dataStore) GetModules(path string) ([]internal.Module, error) {
	// Check if the provided directory exists and is a directory
	dirInfo, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("module directory does not exist: %s", path)
	}
	if !dirInfo.IsDir() {
		return nil, fmt.Errorf("module path is not a directory: %s", path)
	}

	// Read modules inside the provided directory
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("read root module directory: %s", err)
	}

	modules := []internal.Module{}
	for _, modulesDir := range files {
		if modulesDir.IsDir() {
			queryFiles, err := os.ReadDir(path + "/" + modulesDir.Name())
			if err != nil {
				return nil, fmt.Errorf("read module directory: %s", err)
			}
			module := internal.Module{
				Executed: false,
				Name:     modulesDir.Name(),
				Queries:  []internal.ModuleQuery{}}
			for _, queryFile := range queryFiles {
				if queryFile.IsDir() {
					continue
				}
				if filepath.Ext(queryFile.Name()) == ".sql" {
					fileContent, err := os.ReadFile(path + "/" + modulesDir.Name() + "/" + queryFile.Name())
					if err != nil {
						return nil, fmt.Errorf("read query file: %s", err)
					}
					// Get local query params
					paramsFile := path + "/" + modulesDir.Name() + "/" + strings.Split(queryFile.Name(), ".")[0] + "_params.json"
					var params map[string]interface{}
					if _, err := os.Stat(paramsFile); err == nil {
						paramsBytes, err := os.ReadFile(paramsFile)
						if err != nil {
							return nil, fmt.Errorf("read query params file: %s", err)
						}
						err = json.Unmarshal(paramsBytes, &params)
						if err != nil {
							return nil, fmt.Errorf("unmarshal query params: %s", err)
						}
					}
					module.Queries = append(module.Queries, internal.ModuleQuery{
						Name:     queryFile.Name(),
						Query:    string(fileContent),
						Params:   params,
						Executed: false,
					})
				}
			}
			modules = append(modules, module)
		}
	}
	sort.Slice(modules, func(i, j int) bool {
		return modules[i].Name < modules[j].Name
	})
	return modules, nil
}
