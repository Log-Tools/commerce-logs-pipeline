# Commerce Logs Pipeline - Development Guide

This document defines the development standards and project structure for all modules in the Commerce Logs Pipeline workspace.

## Module Structure Standards

All modules in this workspace should follow the **Standard Go Project Layout** to ensure consistency, maintainability, and team productivity.

### Required Directory Structure

```
pipeline/[module-name]/
├── cmd/[module-name]/         # Application entry points
│   └── main.go               # Main function and CLI setup
├── internal/                 # Internal packages (not importable externally)
│   ├── config/              # Configuration management
│   │   ├── config.go        # Config structures and validation
│   │   └── config_test.go   # Configuration unit tests
│   ├── [domain]/            # Domain-specific packages
│   │   ├── [domain].go      # Implementation
│   │   └── [domain]_test.go # Unit tests
│   └── service/             # Core service implementation
│       ├── service.go       # Main service logic
│       └── service_test.go  # Service unit tests with mocks
├── test/                    # Integration tests
│   └── integration_test.go  # Integration tests with external dependencies
├── configs/                 # Configuration files
│   ├── config.yaml         # Default configuration
│   └── config.yaml.example # Example configuration with documentation
├── deployments/             # Deployment configurations
│   ├── Dockerfile          # Multi-stage Docker build
│   └── k8s/                # Kubernetes manifests (if applicable)
├── docs/                    # Additional documentation (optional)
├── scripts/                 # Build and utility scripts (optional)
├── bin/                     # Built binaries (gitignored)
├── Makefile                 # Build, test, and development commands
├── go.mod                   # Go module definition
├── go.sum                   # Dependency checksums
└── README.md               # Module documentation
```

## Architecture Standards

### Dependency Injection

All modules must implement proper dependency injection to ensure testability, maintainability, and adherence to SOLID principles.

#### Interface-Based Design

Define interfaces for all external dependencies:

```go
// Good: Abstract external dependencies behind interfaces
type Producer interface {
    Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
    Events() chan kafka.Event
    Flush(timeoutMs int) int
    Close()
}

type StorageClientFactory interface {
    CreateClients(cfg *Config) (map[string]*Client, error)
}
```

#### Factory Pattern for Dependencies

Use the factory pattern to create dependencies, allowing different implementations for production and testing:

```go
// Production implementation
type AzureStorageClientFactory struct{}

func (f *AzureStorageClientFactory) CreateClients(cfg *Config) (map[string]*Client, error) {
    // Real Azure client creation logic
}

// Test implementation  
type MockStorageClientFactory struct{}

func (f *MockStorageClientFactory) CreateClients(cfg *Config) (map[string]*Client, error) {
    return make(map[string]*Client), nil // Empty map for testing
}
```

#### Constructor Hierarchy

Implement a hierarchy of constructors for different use cases:

```go
// High-level convenience constructors
func NewServiceFromFile(configPath string) (*Service, error) {
    cfg, err := LoadConfig(configPath)
    if err != nil {
        return nil, err
    }
    return NewServiceWithConfig(cfg)
}

func NewServiceWithConfig(cfg *Config) (*Service, error) {
    // Create production dependencies
    producer, err := kafka.NewProducer(kafkaConfig)
    if err != nil {
        return nil, err
    }
    
    storageFactory := &AzureStorageClientFactory{}
    return NewService(cfg, producer, storageFactory)
}

// Core constructor with full dependency injection
func NewService(cfg *Config, producer Producer, storageFactory StorageClientFactory) (*Service, error) {
    // All dependencies are injected
}
```

#### Anti-Patterns to Avoid

❌ **Bad: Special testing constructors**
```go
// Don't create separate constructors for testing
func NewService(cfg *Config, producer Producer) (*Service, error) { /* production */ }
func NewServiceForTesting(cfg *Config, producer Producer) *Service { /* testing */ }
```

✅ **Good: Single constructor with dependency injection**
```go
// Same constructor works for production and testing
func NewService(cfg *Config, producer Producer, factory StorageFactory) (*Service, error) {
    // Works with any implementation of the interfaces
}
```

❌ **Bad: Hard-coded dependencies**
```go
func NewService(cfg *Config) (*Service, error) {
    // Hard to test - creates real Azure clients
    client := azblob.NewClient(...)
}
```

✅ **Good: Injected dependencies**
```go
func NewService(cfg *Config, factory StorageFactory) (*Service, error) {
    // Easy to test - can inject mock factory
    clients, err := factory.CreateClients(cfg)
}
```

## Directory Guidelines

### `/cmd/[module-name]/`
- **Purpose**: Application entry points
- **Contents**: Main functions, CLI setup, signal handling
- **Files**: `main.go` (primary), additional commands if needed
- **Rules**: 
  - Keep minimal - delegate to `internal/service`
  - Handle CLI arguments and graceful shutdown
  - One binary per subdirectory

### `/internal/`
- **Purpose**: Private application packages
- **Benefits**: Cannot be imported by external modules
- **Structure**: Organize by domain/responsibility
- **Common packages**:
  - `config/` - Configuration structures and validation
  - `service/` - Core business logic
  - `[domain]/` - Domain-specific logic (e.g., `selectors/`, `events/`)

### `/internal/config/`
- **Purpose**: Configuration management
- **Required files**:
  - `config.go` - Structures, validation, loading
  - `config_test.go` - Unit tests for configuration logic
- **Best practices**:
  - Use struct tags for YAML/JSON mapping
  - Implement validation methods
  - Support environment variable overrides

### `/internal/service/`
- **Purpose**: Main service implementation
- **Required files**:
  - `service.go` - Core service logic with dependency injection
  - `service_test.go` - Unit tests with mocks
- **Best practices**:
  - Interface-based design for testability
  - Dependency injection using factory pattern
  - Graceful shutdown support
  - No special testing constructors

### `/test/`
- **Purpose**: Integration tests with external dependencies
- **Build tags**: Use `//go:build integration` 
- **Dependencies**: May use testcontainers, real databases, etc.
- **Separation**: Keep separate from unit tests for faster CI

### `/configs/`
- **Purpose**: Configuration files
- **Required files**:
  - `config.yaml` - Default working configuration
  - `config.yaml.example` - Documented example
- **Best practices**:
  - Include comments explaining options
  - Provide sensible defaults
  - Document environment-specific overrides

### `/deployments/`
- **Purpose**: Deployment and orchestration files
- **Common files**:
  - `Dockerfile` - Multi-stage build
  - `k8s/` - Kubernetes manifests
  - `docker-compose.yml` - Local development setup
- **Best practices**:
  - Use multi-stage Docker builds
  - Non-root user in containers
  - Health checks and proper labels

## Testing Standards

### Unit Tests
- **Location**: Alongside source files (`*_test.go`)
- **Scope**: Test individual packages in isolation
- **Dependencies**: Use mocks/stubs for external dependencies
- **Command**: `make test-unit` or `go test ./internal/...`
- **Error Testing**: Check error occurrence, not specific messages

### Integration Tests
- **Location**: `/test/` directory
- **Build tags**: `//go:build integration`
- **Scope**: Test with real external dependencies
- **Dependencies**: May use testcontainers, real services
- **Command**: `make test-integration`

### Test Organization with Dependency Injection
```go
// Unit test example with dependency injection
package service

import (
    "testing"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestServiceMethod(t *testing.T) {
    // Arrange - inject mock dependencies
    config := createTestConfig()
    mockProducer := NewMockProducer()
    mockStorageFactory := &MockStorageClientFactory{}
    
    service, err := NewService(config, mockProducer, mockStorageFactory)
    require.NoError(t, err)
    
    // Act & Assert
    err = service.SomeMethod()
    assert.NoError(t, err)
}

// Test error conditions without brittle string matching
func TestServiceMethod_ErrorHandling(t *testing.T) {
    config := createTestConfig()
    mockProducer := NewMockProducer()
    failingFactory := &FailingStorageFactory{}
    
    service, err := NewService(config, mockProducer, failingFactory)
    assert.Error(t, err) // Check error occurs, not specific message
    assert.Nil(t, service)
}
```

### Error Testing Best Practices

❌ **Bad: Brittle string matching**
```go
assert.Contains(t, err.Error(), "failed to load storage config")
```

✅ **Good: Check error occurrence**
```go
assert.Error(t, err) // Just verify an error occurred
```

✅ **Good: Use error types when available**
```go
var configErr *ConfigError
assert.ErrorAs(t, err, &configErr)
```

✅ **Good: Check error behavior**
```go
assert.True(t, errors.Is(err, ErrConfigNotFound))
```

## Makefile Standards

Every module should include a comprehensive Makefile with these targets:

### Required Targets
```makefile
# Build targets
build                 # Build main binary
dev-build            # Build with race detection
release              # Multi-platform builds
clean                # Clean build artifacts

# Test targets  
test                 # Run all tests
test-unit            # Unit tests only
test-integration     # Integration tests only
test-coverage        # Generate coverage report

# Development targets
run                  # Build and run locally
lint                 # Code linting
format               # Code formatting
deps                 # Download dependencies

# Docker targets
docker-build         # Build Docker image
docker-run           # Build and run container

# Utility targets
help                 # Show available commands
```

### Makefile Template
```makefile
# Module Makefile Template
.PHONY: build clean test run help

BINARY_NAME := [module-name]
CMD_DIR := cmd/$(BINARY_NAME)
BIN_DIR := bin
TARGET := $(BIN_DIR)/$(BINARY_NAME)

build: $(TARGET)

$(TARGET): $(shell find . -name "*.go" | grep -v "_test.go")
	@mkdir -p $(BIN_DIR)
	cd $(CMD_DIR) && go build -o ../../$(TARGET) .

test: test-unit test-integration

test-unit:
	go test -v -race ./internal/...

test-integration:
	go test -v -race -tags=integration ./test/...

clean:
	rm -rf $(BIN_DIR)

help:
	@echo "Available targets:"
	@echo "  build    - Build the binary"
	@echo "  test     - Run all tests"
	@echo "  clean    - Clean build artifacts"
	@echo "  help     - Show this help"
```

## Code Standards

### Import Organization
```go
import (
    // Standard library
    "context"
    "fmt"

    // Third-party packages
    "github.com/stretchr/testify/assert"

    // Local packages
    "github.com/Log-Tools/commerce-logs-pipeline/internal/config"
)
```

### Package Naming
- Use short, lowercase names
- Avoid generic names like `utils`, `common`, `base`
- Name by purpose: `config`, `service`, `events`, `selectors`

### Error Handling
```go
// Wrap errors with context
if err != nil {
    return fmt.Errorf("failed to process blob %s: %w", blobName, err)
}

// Define custom error types for better testing
type ConfigError struct {
    Field   string
    Message string
}

func (e *ConfigError) Error() string {
    return fmt.Sprintf("config error in field %s: %s", e.Field, e.Message)
}
```

### Configuration
```go
// Use struct tags for YAML
type Config struct {
    KafkaBrokers string `yaml:"kafka_brokers" env:"KAFKA_BROKERS"`
    LogLevel     string `yaml:"log_level" env:"LOG_LEVEL" default:"info"`
}
```

### Service Design with Dependency Injection
```go
// Define interfaces for all external dependencies
type Producer interface {
    Produce(msg *Message) error
    Close()
}

type StorageFactory interface {
    CreateClients(cfg *Config) (map[string]*Client, error)
}

// Service struct with injected dependencies
type Service struct {
    config         *Config
    producer       Producer
    storageClients map[string]*Client
}

// Constructor with dependency injection
func NewService(cfg *Config, producer Producer, factory StorageFactory) (*Service, error) {
    clients, err := factory.CreateClients(cfg)
    if err != nil {
        return nil, fmt.Errorf("failed to create storage clients: %w", err)
    }
    
    return &Service{
        config:         cfg,
        producer:       producer,
        storageClients: clients,
    }, nil
}
```

## Documentation Standards

### README.md Structure
Each module should have a comprehensive README with:

1. **Project Description** - What the module does
2. **Project Structure** - Directory layout with explanations
3. **Architecture** - High-level design and data flow with dependency injection patterns
4. **Configuration** - Examples and reference
5. **Usage** - How to build, run, and deploy
6. **Development** - How to add features and run tests
7. **Monitoring** - Logs, metrics, health checks

### Code Documentation
- Public functions and types must have doc comments
- Include examples for complex APIs
- Document configuration options thoroughly
- Document interface contracts and expected behaviors

## Module Examples

### Currently Implemented
- ✅ **blob-monitor**: Follows complete standard structure
  - Proper `/cmd`, `/internal`, `/test` organization
  - Dependency injection with factory pattern
  - Interface-based design for testability
  - Comprehensive Makefile
  - Unit and integration tests
  - Docker support

### Future Modules
All new modules should follow this structure:
- **log-ingestion**: Kafka consumer for blob events
- **log-processor**: Log parsing and transformation
- **log-storage**: Data warehouse integration
- **monitoring**: Metrics and alerting

## Benefits of This Architecture

1. **Testability**: Easy unit testing with mock dependencies
2. **Maintainability**: Clear separation of concerns and interfaces
3. **Flexibility**: Can swap implementations without changing core logic
4. **SOLID Principles**: Follows dependency inversion and single responsibility
5. **Consistency**: All modules follow the same patterns
6. **Professional**: Industry-standard Go project layout and practices
7. **CI/CD**: Standardized build and test processes

## Getting Started

When creating a new module:

1. Create directory: `pipeline/[module-name]/`
2. Copy structure from `pipeline/blob-monitor/` as template
3. Update module name in `go.mod`
4. Implement your domain logic in `internal/`
5. **Design interfaces for external dependencies**
6. **Implement factory pattern for dependency creation**
7. **Use dependency injection in service constructor**
8. Add unit tests alongside code with mocked dependencies
9. Add integration tests in `test/`
10. Update Makefile targets
11. Write comprehensive README

Following these standards ensures all modules are professional, maintainable, and consistent across the Commerce Logs Pipeline workspace. 