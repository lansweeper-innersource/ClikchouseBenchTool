package datastore

import "github.com/lansweeper/ClickhouseBenchTool/internal"

type DataStore interface {
	GetModules(path string) ([]internal.Module, error)
}

type dataStore struct{}

func CreateDataStore() DataStore {
	return &dataStore{}
}
