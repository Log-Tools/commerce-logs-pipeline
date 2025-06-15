# Application Logs Extraction Service Makefile
.PHONY: build clean test run help test-unit test-integration lint format deps docker-build docker-run reset-extraction reset-dry-run

BINARY_NAME := extraction
CMD_DIR := cmd/$(BINARY_NAME)
BIN_DIR := bin
TARGET := $(BIN_DIR)/$(BINARY_NAME)

# Build targets
build: $(TARGET)

$(TARGET): $(shell find . -name "*.go" | grep -v "_test.go")
	@mkdir -p $(BIN_DIR)
	cd $(CMD_DIR) && go build -o ../../$(TARGET) .

dev-build: 
	@mkdir -p $(BIN_DIR)
	cd $(CMD_DIR) && go build -race -o ../../$(TARGET) .

release:
	@mkdir -p $(BIN_DIR)
	cd $(CMD_DIR) && GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o ../../$(BIN_DIR)/$(BINARY_NAME)-linux-amd64 .
	cd $(CMD_DIR) && GOOS=darwin GOARCH=amd64 go build -ldflags="-s -w" -o ../../$(BIN_DIR)/$(BINARY_NAME)-darwin-amd64 .
	cd $(CMD_DIR) && GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o ../../$(BIN_DIR)/$(BINARY_NAME)-windows-amd64.exe .

clean:
	rm -rf $(BIN_DIR)

# Test targets
test: test-unit test-integration

test-unit:
	go test -v -race ./internal/...

test-integration:
	go test -v -race -tags=integration ./test/...

test-coverage:
	go test -v -race -coverprofile=coverage.out ./internal/...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Development targets
run: build
	./$(TARGET) configs/config.yaml

lint:
	golangci-lint run ./...

format:
	go fmt ./...
	goimports -w .

deps:
	go mod download
	go mod tidy

# Docker targets
docker-build:
	docker build -t extraction-service .

docker-run: docker-build
	docker run --rm -it \
		-v $(PWD)/configs:/app/configs \
		extraction-service

# Reset/cleanup targets
reset-extraction:
	@echo "🔄 Resetting extraction phase for reprocessing..."
	@echo "⚠️  This will:"
	@echo "   - Clear Extracted.Application topic"
	@echo "   - Clear Extraction.Errors topic"  
	@echo "   - Reset consumer group 'extraction-service' to beginning of Raw.ApplicationLogs"
	@echo ""
	@if ! docker ps | grep -q kafka; then \
		echo "❌ Kafka container not running. Start with: make dev-up"; \
		exit 1; \
	fi
	@read -p "Continue? (y/N): " confirm && [ "$$confirm" = "y" ] || { echo "Cancelled."; exit 1; }
	@echo ""
	@echo "🗑️  Clearing extraction output topics..."
	@docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic Extracted.Application 2>/dev/null || echo "   (topic Extracted.Application doesn't exist)"
	@docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic Extraction.Errors 2>/dev/null || echo "   (topic Extraction.Errors doesn't exist)"
	@echo "🔄 Resetting consumer group offset..."
	@docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group extraction-service --reset-offsets --topic Raw.ApplicationLogs --to-earliest --execute 2>/dev/null || echo "   (consumer group doesn't exist yet)"
	@echo "🛠️  Recreating topics with proper configuration..."
	@sleep 2
	@cd ../../ && make init-kafka
	@echo "✅ Extraction reset complete!"
	@echo "   - All extraction output cleared"
	@echo "   - Consumer will start from beginning of Raw.ApplicationLogs"
	@echo "   - Ready to reprocess all logs"

reset-dry-run:
	@echo "🔍 Dry run: What reset-extraction would do"
	@echo "=========================================="
	@echo "📊 Current state:"
	@if docker ps | grep -q kafka; then \
		echo "   Topics that would be deleted:"; \
		docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -E "(Extracted\.Application|Extraction\.Errors)" | sed 's/^/     - /' || echo "     (no extraction topics found)"; \
		echo "   Consumer group offsets:"; \
		docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group extraction-service 2>/dev/null | grep Raw.ApplicationLogs | sed 's/^/     /' || echo "     (no consumer group found)"; \
	else \
		echo "   ❌ Kafka not running"; \
	fi
	@echo ""
	@echo "🔄 Actions that would be performed:"
	@echo "   1. Delete topic: Extracted.Application"
	@echo "   2. Delete topic: Extraction.Errors"
	@echo "   3. Reset consumer group 'extraction-service' to earliest offset on Raw.ApplicationLogs"
	@echo "   4. Recreate topics with proper configuration"

# Utility targets
help:
	@echo "Available targets:"
	@echo "  build              - Build the binary"
	@echo "  dev-build          - Build with race detection"
	@echo "  release            - Multi-platform builds"
	@echo "  clean              - Clean build artifacts"
	@echo "  test               - Run all tests"
	@echo "  test-unit          - Unit tests only"
	@echo "  test-integration   - Integration tests only"
	@echo "  test-coverage      - Generate coverage report"
	@echo "  run                - Build and run locally"
	@echo "  lint               - Run linter"
	@echo "  format             - Format code"  
	@echo "  deps               - Download dependencies"
	@echo "  docker-build       - Build Docker image"
	@echo "  docker-run         - Build and run container"
	@echo "  reset-extraction   - Reset extraction phase for reprocessing"
	@echo "  reset-dry-run      - Show what reset would do (dry run)"
	@echo "  help               - Show this help" 