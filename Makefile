.PHONY: build run test lint fmt clean test-race test-cover test-chaos

BINARY=bin/quorum

build:
	go build -o $(BINARY) ./cmd/quorum

run: build
	./$(BINARY)

test:
	go test -v -short ./...

test-race:
	go test -v -race -short ./...

test-cover:
	go test -v -race -short -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

test-chaos:
	go test -v -race -timeout 5m ./internal/chaos/

lint:
	golangci-lint run

fmt:
	gofmt -s -w .
	go mod tidy

clean:
	rm -rf bin/ data/ coverage.out coverage.html
	go clean -testcache

cluster:
	@echo "Starting 3-node cluster..."
	@go run ./cmd/quorum --id=node-1 --port=9001 --http=8001 &
	@go run ./cmd/quorum --id=node-2 --port=9002 --http=8002 &
	@go run ./cmd/quorum --id=node-3 --port=9003 --http=8003 &
