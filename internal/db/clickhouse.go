package db

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type ClickHouseConfig struct {
	Port     int
	Host     string
	Database string
	Username string
	Password string
	Secure   bool
}

func GetClickHouse(ctx context.Context, clickHouseConfig ClickHouseConfig) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", clickHouseConfig.Host, clickHouseConfig.Port)},
		Auth: clickhouse.Auth{
			Database: clickHouseConfig.Database,
			Username: clickHouseConfig.Username,
			Password: clickHouseConfig.Password,
		},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "clickhouse-benchmark-tool", Version: "1.0"},
			},
		},
		Debugf: func(format string, v ...interface{}) {
			fmt.Printf(format, v)
		},
		TLS: &tls.Config{
			InsecureSkipVerify: true,
		},
	})

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, err
}
