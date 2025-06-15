# Blob Monitor Service Makefile
.PHONY: build clean install test test-unit test-integration run dev-build docker-build docker-run lint format help

	# Configuration
BINARY_NAME := blob-monitor
BIN_DIR := bin
TARGET := $(BIN_DIR)/$(BINARY_NAME)
CMD_DIR := cmd/blob-monitor
DOCKER_IMAGE := commerce-blob-monitor:latest

# Build information
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

# Go build flags
LDFLAGS := -X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)
BUILD_FLAGS := -ldflags "$(LDFLAGS)"

# Default target
build: $(TARGET)

# Build the main binary
$(TARGET): $(shell find . -name "*.go" | grep -v "_test.go")
	@echo "🔨 Building $(BINARY_NAME) v$(VERSION)..."
	@mkdir -p $(BIN_DIR)
	cd $(CMD_DIR) && go build $(BUILD_FLAGS) -o ../../$(TARGET) .
	@echo "✅ Built $(TARGET)"

# Clean build artifacts
clean:
	@echo "🧹 Cleaning $(BINARY_NAME)..."
	rm -rf $(BIN_DIR)
	go clean -cache
	@echo "✅ Cleaned"

# Install to GOPATH
install:
	@echo "📦 Installing $(BINARY_NAME)..."
	cd $(CMD_DIR) && go install $(BUILD_FLAGS) .

# Run all tests
test: test-unit

# Run unit tests (fast tests without external dependencies)
test-unit:
	@echo "🧪 Running unit tests..."
	go test -v -race -count=1 ./internal/...

# Run integration tests (tests with Kafka, containers, etc.)
test-integration:
	@echo "🧪 Running integration tests..."
	@echo "⚠️  Integration tests require Docker for testcontainers"
	go test -v -race -count=1 -tags=integration -timeout=5m ./...

# Run tests with coverage
test-coverage:
	@echo "🧪 Running tests with coverage..."
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "📊 Coverage report generated: coverage.html"

# Run the service locally
run: build
	@echo "🚀 Running $(BINARY_NAME)..."
	./$(TARGET) configs/config.yaml

# Development build with race detection
dev-build: 
	@echo "🔨 Building $(BINARY_NAME) (dev mode)..."
	@mkdir -p $(BIN_DIR)
	cd $(CMD_DIR) && go build -race $(BUILD_FLAGS) -o ../../$(TARGET) .

# Lint the code
lint:
	@echo "🔍 Linting code..."
	golangci-lint run ./...

# Format the code
format:
	@echo "🎨 Formatting code..."
	go fmt ./...
	goimports -w .

# Build Docker image
docker-build:
	@echo "🐳 Building Docker image..."
	docker build -f deployments/Dockerfile -t $(DOCKER_IMAGE) .

# Run Docker container
docker-run: docker-build
	@echo "🐳 Running Docker container..."
	docker run --rm -v $(PWD)/configs:/app/configs $(DOCKER_IMAGE) configs/config.yaml

# Download dependencies
deps:
	@echo "📦 Downloading dependencies..."
	go mod download
	go mod tidy

# Verify dependencies
verify:
	@echo "🔍 Verifying dependencies..."
	go mod verify

# Run benchmarks
bench:
	@echo "⚡ Running benchmarks..."
	go test -bench=. -benchmem ./...

# Generate mocks (if using mockgen)
mocks:
	@echo "🎭 Generating mocks..."
	go generate ./...

# Create release build
release: clean
	@echo "📦 Creating release build..."
	@mkdir -p $(BIN_DIR)
	cd $(CMD_DIR) && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(BUILD_FLAGS) -a -installsuffix cgo -o ../../$(BIN_DIR)/$(BINARY_NAME)-linux-amd64 .
	cd $(CMD_DIR) && CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build $(BUILD_FLAGS) -a -installsuffix cgo -o ../../$(BIN_DIR)/$(BINARY_NAME)-darwin-amd64 .
	cd $(CMD_DIR) && CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build $(BUILD_FLAGS) -a -installsuffix cgo -o ../../$(BIN_DIR)/$(BINARY_NAME)-windows-amd64.exe .
	@echo "✅ Release builds created in $(BIN_DIR)/"

# Setup development environment
setup-dev:
	@echo "🛠️  Setting up development environment..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install golang.org/x/tools/cmd/goimports@latest
	@echo "✅ Development tools installed"

# Show help
help:
	@echo "Blob Monitor Service Commands:"
	@echo ""
	@echo "Build Commands:"
	@echo "  build        Build the binary to bin/"
	@echo "  dev-build    Build with race detection"
	@echo "  release      Create multi-platform release builds"
	@echo "  docker-build Build Docker image"
	@echo ""
	@echo "Test Commands:"
	@echo "  test         Run all tests"
	@echo "  test-unit    Run unit tests only"
	@echo "  test-integration Run integration tests with containers"
	@echo "  test-coverage Run tests with coverage report"
	@echo "  bench        Run benchmarks"
	@echo ""
	@echo "Run Commands:"
	@echo "  run          Build and run with configs/config.yaml"
	@echo "  docker-run   Build and run in Docker container"
	@echo ""
	@echo "Development Commands:"
	@echo "  lint         Run linter"
	@echo "  format       Format code"
	@echo "  deps         Download dependencies"
	@echo "  verify       Verify dependencies"
	@echo "  setup-dev    Install development tools"
	@echo ""
	@echo "Utility Commands:"
	@echo "  clean        Remove bin/ directory"
	@echo "  install      Install to GOPATH/bin"
	@echo "  help         Show this help" 