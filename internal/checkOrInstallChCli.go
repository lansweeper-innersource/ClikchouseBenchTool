package internal

import (
	"fmt"
	"os"
	"os/exec"
)

func CheckOrInstallCHCli() (string, error) {

	// check if executable exists locally and, if it is, check if it is a valid ClickHouse client
	_, err := os.Stat("clickhouse")
	if err == nil {
		chVersionCmd := exec.Command("./clickhouse", "client", "--version")
		if err := chVersionCmd.Run(); err == nil {
			return "./clickhouse", nil
		}
	}
	fmt.Println("Downloading clickhouse CLI ...")

	installCmd := exec.Command("curl", "https://clickhouse.com/")
	outfile, err := os.CreateTemp("", "clickhouse-install.sh")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer outfile.Close()
	installCmd.Stdout = outfile

	err = installCmd.Start()
	if err != nil {
		return "", fmt.Errorf("failed to start install command: %w", err)
	}
	err = installCmd.Wait()
	if err != nil {
		return "", fmt.Errorf("failed to wait for install command: %w", err)
	}

	installCmd = exec.Command("sh", outfile.Name())
	err = installCmd.Run()
	if err != nil {
		return "", fmt.Errorf("failed to run install command: %w", err)
	}

	return "./clickhouse", nil
}
