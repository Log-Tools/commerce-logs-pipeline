# Application Logs Extraction Service

A scalable Kafka stream processor that extracts structured data from raw application logs, converting JSON-formatted log entries into structured events for downstream processing.

## Project Description

The extraction service processes raw application logs from Kubernetes environments and transforms them into structured events. It parses JSON log entries, extracts essential fields like timestamps, log levels, messages, and exception information, and publishes them to Kafka topics for further processing in the logs-derived trace pipeline.

## Project Structure

```
pipeline/extraction/
├── cmd/extraction/                # Application entry point
│   └── main.go                   # Main function and CLI setup  
├── internal/                     # Internal packages
│   ├── config/                   # Configuration management
│   │   ├── config.go            # Config structures and validation
│   │   └── config_test.go       # Configuration unit tests
│   ├── extractor/               # Log extraction logic  
│   │   ├── extractor.go         # Core extraction implementation
│   │   └── extractor_test.go    # Extraction unit tests
│   └── service/                 # Core service implementation
│       ├── service.go           # Main service logic with dependency injection
│       ├── service_test.go      # Service unit tests with mocks
│       ├── interfaces.go        # Interface definitions for DI
│       └── factories.go         # Production factory implementations
├── test/                        # Integration tests
├── configs/                     # Configuration files
│   └── config.yaml             # Example configuration with documentation
├── deployments/                 # Deployment configurations
├── bin/                         # Built binaries (gitignored)
├── Makefile                     # Build, test, and development commands
├── go.mod                       # Go module definition
├── go.sum                       # Dependency checksums
└── README.md                   # This file
```

## Architecture

### High-Level Data Flow

```
Raw Application Logs (Kafka) → Extraction Service → Structured Events (Kafka)
                                      ↓
                              Error Events (Kafka)
```

### Dependency Injection Design

The service uses interface-based dependency injection for testability and maintainability:

- **Consumer/Producer Interfaces**: Abstract Kafka operations
- **Extractor Interface**: Abstract log parsing and validation
- **MetricsCollector Interface**: Abstract metrics collection
- **Factory Pattern**: Create dependencies with different implementations for production/testing

### Concurrent Processing Architecture

The service uses a **concurrent worker-based architecture** for high-throughput processing:

1. **Main Consumer Loop**: Single goroutine reads messages from Kafka and distributes to workers
2. **Worker Pool**: Configurable number of worker goroutines (default: 12, matching Kafka partition count)
3. **Message Channel**: Buffered channel coordinates message distribution with backpressure
4. **Partition Optimization**: 12 workers match the 12 partitions of `Raw.ApplicationLogs` for maximum parallelism

### Processing Flow

1. **Message Consumption**: Main loop reads raw log lines from `Raw.ApplicationLogs` topic
2. **Message Distribution**: Messages sent to available workers via buffered channel
3. **Concurrent Extraction**: Workers parse JSON structure and extract essential fields in parallel
4. **Validation**: Workers validate extracted data (optional)
5. **Event Publishing**: Workers send structured events to `Extracted.Application` topic
6. **Error Handling**: Workers send extraction errors to `Extraction.Errors` topic
7. **Backpressure**: System blocks reading when workers are overwhelmed (no message dropping)

## Configuration

The service uses YAML configuration with comprehensive validation. See `configs/config.yaml` for a complete example.

### Key Configuration Sections

```yaml
kafka:
  brokers: "localhost:9092"
  input_topic: "Raw.ApplicationLogs"    # Input topic
  output_topic: "Extracted.Application" # Output topic for structured events
  error_topic: "Extraction.Errors"      # Output topic for extraction errors
  consumer_group: "extraction-service"

processing:
  max_concurrency: 12                   # Worker goroutines (should match input topic partitions)
  enable_validation: true               # Validate extracted logs
  skip_invalid_logs: false              # Send errors to error topic
  log_parse_errors: true                # Log errors to console

logging:
  level: "info"                         # Log level
  enable_metrics: true                  # Enable metrics collection
  metrics_interval: 60                  # Metrics reporting interval (seconds)
```

### Environment-Specific Configuration

Update the configuration values for different environments:

- **Development**: Use local Kafka, enable debug logging
- **Production**: Use production Kafka clusters, optimize batch sizes and timeouts

## Usage

### Prerequisites

- Go 1.23+
- Kafka cluster running
- Topics created: `Raw.ApplicationLogs`, `Extracted.Application`, `Extraction.Errors`

### Building and Running

```bash
# Build the service
make build

# Run with default configuration
make run

# Run with custom configuration
./bin/extraction /path/to/your/config.yaml

# Build with race detection for development
make dev-build

# Multi-platform release builds
make release
```

### Docker Usage

```bash
# Build Docker image
make docker-build

# Run container with mounted config
make docker-run

# Or run manually
docker run --rm -it \
  -v $(PWD)/configs:/app/configs \
  extraction-service
```

### Testing Kafka Connectivity

The service will validate Kafka connectivity on startup and fail fast if unable to connect.

## Development

### Running Tests

```bash
# Run all tests
make test

# Unit tests only (fast)
make test-unit

# Integration tests (requires external dependencies)
make test-integration

# Generate coverage report
make test-coverage
```

### Code Quality

```bash
# Format code
make format

# Run linter
make lint

# Update dependencies
make deps
```

### Adding New Features

1. **Extraction Logic**: Extend `internal/extractor/extractor.go`
2. **Service Logic**: Modify `internal/service/service.go`
3. **Configuration**: Update `internal/config/config.go`
4. **Tests**: Add corresponding unit tests following the existing patterns

### Testing Strategy

- **Unit Tests**: Test individual packages with mocked dependencies
- **Integration Tests**: Test with real Kafka (use Docker/testcontainers)
- **Dependency Injection**: All external dependencies are mocked for unit testing

## Input/Output Format

### Input: Raw Application Logs

```json
{
  "@timestamp": "2024-01-01T10:00:00.000Z",
  "logs": {
    "instant": {
      "epochSecond": 1735606800,
      "nanoOfSecond": 123456789
    },
    "level": "INFO",
    "loggerName": "com.example.Service",
    "thread": "main",
    "message": "Processing request",
    "thrown": "java.lang.Exception: Error details"
  },
  "kubernetes": {
    "pod_name": "api-service-674b9bdb88-vm775",
    "namespace_name": "default",
    "container_name": "app"
  }
}
```

### Output: Structured Application Log

```json
{
  "ts_ns": 1735606800123456789,
  "level": "INFO",
  "logger": "com.example.Service",
  "thread": "main",
  "msg": "Processing request",
  "thrown": "java.lang.Exception: Error details",
  "raw_line": "...",
  "source": {
    "service": "api-service",
    "environment": "P1",
    "subscription": "cp2",
    "pod_name": "api-service-674b9bdb88-vm775",
    "container": "app",
    "namespace": "default"
  }
}
```

### Output: Extraction Error

```json
{
  "raw_line": "invalid log line",
  "error": "failed to parse JSON: invalid character...",
  "error_type": "parse_error",
  "timestamp": 1735606800123456789,
  "source": {
    "service": "unknown",
    "environment": "P1",
    "subscription": "cp2"
  }
}
```

## Monitoring

### Metrics

The service collects processing metrics:

- Messages processed count
- Messages extracted count  
- Extraction errors count
- Validation errors count
- Processing latency histogram

### Health Checks

- Kafka connectivity validation on startup
- Consumer group health monitoring
- Producer delivery confirmation

### Logging

Structured logging with configurable levels:

- **DEBUG**: Detailed processing information
- **INFO**: Service lifecycle and processing stats
- **WARN**: Recoverable errors and unusual conditions  
- **ERROR**: Service errors and failures

## Troubleshooting

### Common Issues

1. **Kafka Connection Failures**
   - Verify broker addresses in configuration
   - Check network connectivity and firewall rules
   - Ensure topics exist with proper permissions

2. **High Processing Latency**
   - Increase `max_concurrency` for parallel processing
   - Optimize Kafka producer configuration
   - Check downstream consumer capacity

3. **High Error Rates**
   - Enable `log_parse_errors` to see error details
   - Review raw log format changes
   - Check service name extraction patterns

### Performance Tuning

- **Kafka Consumer**: Adjust `batch_size` and consumer configuration
- **Processing**: Tune `max_concurrency` based on CPU cores
- **Memory**: Monitor memory usage with complex exception parsing

## Contributing

Follow the patterns established in this service:

1. Use dependency injection for testability
2. Implement comprehensive unit tests with mocks
3. Add integration tests for external dependencies
4. Follow the project's error handling patterns
5. Update configuration and documentation 