# Raw Log Ingestion Pipeline Makefile

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
BINARY_NAME=ingest
BINARY_UNIX=$(BINARY_NAME)_unix
DOCKER_IMAGE=commerce-logs-ingest

# Directories
CMD_DIR=./cmd/ingest
BIN_DIR=./bin
CONFIG_DIR=./configs

# Build the application
build:
	$(GOBUILD) -o $(BIN_DIR)/$(BINARY_NAME) -v $(CMD_DIR)

# Test targets following DEVELOPMENT.md standards
test: test-unit test-integration

test-unit:
	$(GOTEST) -v -race ./internal/...

test-integration:
	$(GOTEST) -v -race -tags=integration ./test/...

test-coverage:
	$(GOTEST) -v -race -coverprofile=coverage.out ./internal/...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

# Clean build artifacts
clean:
	$(GOCLEAN)
	rm -f $(BIN_DIR)/$(BINARY_NAME)
	rm -f $(BIN_DIR)/$(BINARY_UNIX)
	rm -f coverage.out coverage.html

# Cross compilation for Linux
build-linux:
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 $(GOBUILD) -o $(BIN_DIR)/$(BINARY_UNIX) -v $(CMD_DIR)

# Run in CLI mode
run-cli: build
	$(BIN_DIR)/$(BINARY_NAME) -mode=cli

# Run in worker mode
run-worker: build
	$(BIN_DIR)/$(BINARY_NAME) -mode=worker

# Run with configuration file
run-cli-config: build
	$(BIN_DIR)/$(BINARY_NAME) -config=$(CONFIG_DIR)/cli-example.yaml

run-worker-config: build
	$(BIN_DIR)/$(BINARY_NAME) -config=$(CONFIG_DIR)/worker-example.yaml

# Test Kafka connection
test-kafka: build
	$(BIN_DIR)/$(BINARY_NAME) -test-kafka

test-kafka-custom: build
	$(BIN_DIR)/$(BINARY_NAME) -test-kafka -brokers="broker1:9092,broker2:9092"

# Docker commands
docker-build:
	docker build -t $(DOCKER_IMAGE) .

# Run Docker container in CLI mode (set environment variables in your shell)
docker-run-cli: docker-build
	docker run -e INGEST_MODE=cli $(DOCKER_IMAGE)

# Run Docker container in worker mode (set environment variables in your shell)
docker-run-worker: docker-build
	docker run -e INGEST_MODE=worker $(DOCKER_IMAGE)

# Development helpers
deps:
	$(GOMOD) download
	$(GOMOD) tidy

# Lint the code (requires golangci-lint)
lint:
	golangci-lint run

# Format the code
fmt:
	$(GOCMD) fmt ./...

# Security scan (requires gosec)
security:
	gosec ./...

# Generate mocks (requires mockery)
mocks:
	mockery --all --output ./test/mocks

# Run all checks before commit
pre-commit: fmt lint test security

# Setup development environment
dev-setup:
	$(GOGET) github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	$(GOGET) github.com/securecodewarrior/gosec/v2/cmd/gosec@latest
	$(GOGET) github.com/vektra/mockery/v2@latest

# Show help
help:
	@echo "Available targets:"
	@echo "  build              - Build the application"
	@echo "  test               - Run tests"
	@echo "  test-coverage      - Run tests with coverage"
	@echo "  clean              - Clean build artifacts"
	@echo "  build-linux        - Cross compile for Linux"
	@echo "  run-cli            - Run in CLI mode"
	@echo "  run-worker         - Run in worker mode"
	@echo "  run-cli-config     - Run CLI mode with config file"
	@echo "  run-worker-config  - Run worker mode with config file"
	@echo "  test-kafka         - Test Kafka connection"
	@echo "  test-kafka-custom  - Test custom Kafka brokers"
	@echo "  docker-build       - Build Docker image"
	@echo "  docker-run-cli     - Run Docker container in CLI mode"
	@echo "  docker-run-worker  - Run Docker container in worker mode"
	@echo "  deps               - Download and tidy dependencies"
	@echo "  lint               - Lint the code"
	@echo "  fmt                - Format the code"
	@echo "  security           - Run security scan"
	@echo "  mocks              - Generate mocks"
	@echo "  pre-commit         - Run all checks before commit"
	@echo "  dev-setup          - Setup development environment"

.PHONY: build test test-coverage clean build-linux run-cli run-worker run-cli-config run-worker-config test-kafka test-kafka-custom docker-build docker-run-cli docker-run-worker deps lint fmt security mocks pre-commit dev-setup help 