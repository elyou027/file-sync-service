.PHONY: build build-linux build-all clean

APP_NAME=file-sync-service
VERSION?=latest

build:
	go build -o $(APP_NAME) main.go

build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -ldflags '-extldflags "-static" -w -s' -o $(APP_NAME)-linux-amd64 main.go

build-all:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -ldflags '-extldflags "-static" -w -s' -o $(APP_NAME)-linux-amd64 main.go
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -a -ldflags '-extldflags "-static" -w -s' -o $(APP_NAME)-linux-arm64 main.go
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -a -ldflags '-w -s' -o $(APP_NAME)-darwin-amd64 main.go
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -a -ldflags '-w -s' -o $(APP_NAME)-darwin-arm64 main.go

clean:
	rm -f $(APP_NAME)*

install-deps:
	go mod tidy
