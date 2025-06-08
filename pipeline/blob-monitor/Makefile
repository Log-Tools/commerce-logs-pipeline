# Blob Monitor Module Makefile
.PHONY: build clean install test run

BINARY_NAME := blob-monitor
BIN_DIR := bin
TARGET := $(BIN_DIR)/$(BINARY_NAME)

# Default target
build: $(TARGET)

$(TARGET): *.go
	@echo "🔨 Building $(BINARY_NAME)..."
	@mkdir -p $(BIN_DIR)
	go build -o $(TARGET) .
	@echo "✅ Built $(TARGET)"

clean:
	@echo "🧹 Cleaning $(BINARY_NAME)..."
	rm -rf $(BIN_DIR)
	@echo "✅ Cleaned"

install:
	@echo "📦 Installing $(BINARY_NAME)..."
	go install .

test:
	@echo "🧪 Testing $(BINARY_NAME)..."
	go test ./...

run: build
	@echo "🚀 Running $(BINARY_NAME)..."
	./$(TARGET) config.yaml

# Development helpers
dev-build: 
	go build -race -o $(TARGET) .

help:
	@echo "Blob Monitor Module Commands:"
	@echo "  build     Build the binary to bin/"
	@echo "  clean     Remove bin/ directory"
	@echo "  install   Install to GOPATH/bin"
	@echo "  test      Run tests"
	@echo "  run       Build and run with config.yaml"
	@echo "  dev-build Build with race detection" 