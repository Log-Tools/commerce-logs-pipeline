# Log Tail CLI Makefile

.PHONY: all build test clean install help deps
.DEFAULT_GOAL := help

# Build variables
BINARY_NAME := log-tail
BUILD_DIR := ./bin
MAIN_FILE := main.go

# Go variables
GO := go
GOOS := $(shell $(GO) env GOOS)
GOARCH := $(shell $(GO) env GOARCH)

# Version info
VERSION := $(shell git describe --tags --dirty --always 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Build flags
LDFLAGS := -ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -X main.Commit=$(COMMIT)"

all: deps build ## Install dependencies and build the binary

build: ## Build the binary
	@echo "Building $(BINARY_NAME) for $(GOOS)/$(GOARCH)..."
	@mkdir -p $(BUILD_DIR)
	$(GO) build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) $(MAIN_FILE)
	@echo "Built $(BUILD_DIR)/$(BINARY_NAME)"

build-local: ## Build the binary in the current directory
	@echo "Building $(BINARY_NAME)..."
	$(GO) build $(LDFLAGS) -o $(BINARY_NAME) $(MAIN_FILE)
	@echo "Built $(BINARY_NAME)"

test: ## Run tests
	$(GO) test -v ./...

test-cover: ## Run tests with coverage
	$(GO) test -v -cover -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

deps: ## Install/update dependencies
	$(GO) mod tidy
	$(GO) mod download

clean: ## Clean build artifacts
	@echo "Cleaning build artifacts..."
	@rm -rf $(BUILD_DIR)
	@rm -f $(BINARY_NAME)
	@rm -f coverage.out coverage.html
	@echo "Clean complete"

install: build ## Build and install to $GOPATH/bin
	@echo "Installing $(BINARY_NAME) to $$($(GO) env GOPATH)/bin..."
	@cp $(BUILD_DIR)/$(BINARY_NAME) $$($(GO) env GOPATH)/bin/
	@echo "Installation complete"

install-local: build-local ## Build and install to /usr/local/bin (requires sudo)
	@echo "Installing $(BINARY_NAME) to /usr/local/bin..."
	@sudo cp $(BINARY_NAME) /usr/local/bin/
	@echo "Installation complete"

cross-compile: ## Build for multiple platforms
	@echo "Cross-compiling for multiple platforms..."
	@mkdir -p $(BUILD_DIR)
	@echo "Building for Linux..."
	GOOS=linux GOARCH=amd64 $(GO) build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 $(MAIN_FILE)
	@echo "Building for macOS..."
	GOOS=darwin GOARCH=amd64 $(GO) build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-amd64 $(MAIN_FILE)
	GOOS=darwin GOARCH=arm64 $(GO) build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-arm64 $(MAIN_FILE)
	@echo "Building for Windows..."
	GOOS=windows GOARCH=amd64 $(GO) build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-windows-amd64.exe $(MAIN_FILE)
	@echo "Cross-compilation complete"

run: build-local ## Build and run with example args
	@echo "Running $(BINARY_NAME) --help..."
	./$(BINARY_NAME) --help

setup-config: ## Setup example configuration
	@echo "Setting up example configuration..."
	@mkdir -p ~/.commerce-logs-pipeline
	@if [ ! -f ~/.commerce-logs-pipeline/config.yaml ]; then \
		cp config.yaml.example ~/.commerce-logs-pipeline/config.yaml; \
		echo "Example config copied to ~/.commerce-logs-pipeline/config.yaml"; \
		echo "Please edit this file with your actual configuration"; \
	else \
		echo "Configuration already exists at ~/.commerce-logs-pipeline/config.yaml"; \
	fi

fmt: ## Format Go code
	$(GO) fmt ./...

vet: ## Run go vet
	$(GO) vet ./...

lint: ## Run golangci-lint (requires golangci-lint to be installed)
	@which golangci-lint > /dev/null || (echo "golangci-lint not found. Install with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest" && exit 1)
	golangci-lint run

check: fmt vet ## Run format and vet checks

release: clean cross-compile ## Create a release build
	@echo "Creating release artifacts..."
	@cd $(BUILD_DIR) && for binary in $(BINARY_NAME)-*; do \
		if [[ $$binary == *.exe ]]; then \
			zip $${binary%.exe}.zip $$binary; \
		else \
			tar -czf $$binary.tar.gz $$binary; \
		fi; \
	done
	@echo "Release artifacts created in $(BUILD_DIR)/"

help: ## Show this help message
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

# Development targets
dev-run: ## Run in development mode with live reload (requires air)
	@which air > /dev/null || (echo "air not found. Install with: go install github.com/cosmtrek/air@latest" && exit 1)
	air

dev-deps: ## Install development dependencies
	$(GO) install github.com/cosmtrek/air@latest
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

.PHONY: version
version: ## Show version information
	@echo "Version: $(VERSION)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Commit: $(COMMIT)"
	@echo "Go Version: $$($(GO) version)"
	@echo "OS/Arch: $(GOOS)/$(GOARCH)" 