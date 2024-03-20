dev:
	go run main.go $@

build-mac:
	env GOOS=darwin GOARCH=arm64 go build -o chbt-mac-arm64

build-mac-intel:
	env GOOS=darwin GOARCH=amd64 go build -o chbt-mac-intel

build-linux:
	env GOOS=linux GOARCH=amd64 go build -o chbt-linux

build-windows:
	env GOOS=windows GOARCH=amd64 go build -o chbt-windows.exe

build-all: build-mac build-mac-intel build-linux build-windows
