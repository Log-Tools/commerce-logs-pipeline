# Blob Monitor Service

This service continuously monitors Azure Blob Storage for new log files from commerce cloud environments and publishes events to Kafka for downstream processing.

## Overview

The Blob Monitor Service discovers new log blobs across multiple environments and publishes three types of events:
- **BlobObserved Events**: Individual blob discovery events
- **BlobsListed Events**: Completion events indicating all blobs for a date/service/environment have been listed
- **BlobClosed Events**: Signals that a blob is considered closed (no new data expected) after being inactive for a configurable timeout period

## Project Structure

```
pipeline/blob-monitor/
├── cmd/blob-monitor/          # Application entry point
│   └── main.go               # Main function and CLI setup
├── internal/                 # Internal packages (not importable externally)
│   ├── config/              # Configuration management
│   │   ├── config.go        # Config structures and validation
│   │   └── config_test.go   # Configuration unit tests
│   ├── events/              # Event type definitions
│   │   └── events.go        # BlobObserved and BlobsListed events
│   ├── selectors/           # Blob selector logic
│   │   ├── selectors.go     # Service-specific blob filtering
│   │   └── selectors_test.go # Selector unit tests
│   └── service/             # Core service implementation
│       ├── service.go       # Main service logic
│       └── service_test.go  # Service unit tests with mocks
├── test/                    # Integration tests
│   └── integration_test.go  # Kafka integration tests with testcontainers
├── configs/                 # Configuration files
│   └── config.yaml         # Default configuration
├── deployments/             # Deployment configurations
│   └── Dockerfile          # Multi-stage Docker build
├── bin/                     # Built binaries (gitignored)
├── Makefile                 # Build, test, and development commands
├── go.mod                   # Go module definition
├── go.sum                   # Dependency checksums
└── README.md               # This documentation
```

## Architecture

```
Azure Blob Storage → Blob Monitor Service → Kafka Topic (Ingestion.Blobs)
```

### Key Features

- **Multi-Environment Monitoring**: Simultaneously monitor dev, staging, and production environments
- **Service-Specific Filtering**: Use configurable blob selectors to identify different service types
- **Historical Backfill**: Scan past dates for missing blobs during initial setup
- **Current Day Monitoring**: Continuous polling for new blobs on the current day
- **End-of-Day Handling**: Smart overlap period to handle timezone issues and delayed blob creation
- **Configurable Polling**: Different polling intervals per environment
- **Blob Closing Detection**: Automatically detect when blobs are no longer being written to
- **Robust Error Handling**: Retry logic and graceful degradation

## Configuration

### Example Configuration File

```yaml
# configs/config.yaml
kafka:
  brokers: "localhost:9092"
  topic: "Ingestion.Blobs"
  producer_config:
    acks: "all"
    retries: 3

global:
  polling_interval: 300  # 5 minutes
  eod_overlap_minutes: 60  # 1 hour overlap
  timezone: "UTC"
  blob_closing:
    enabled: true
    timeout_minutes: 5  # Consider blob closed after 5 minutes of inactivity

date_range:
  days_back: 3  # Look back 3 days
  monitor_current_day: true

environments:
  - subscription: "cp2"
    environment: "D1"
    enabled: true
    polling_interval: 600  # Override: 10 minutes for dev
    selectors:
      - "apache-proxy"
      - "api-service"
      - "zookeeper"

storage:
  container_name: "commerce-logs-separated"

monitoring:
  log_level: "info"
  enable_metrics: true
```

## Blob Selectors

Blob selectors are defined in `internal/selectors/selectors.go` and combine Azure prefix with predicate functions:

### Available Selectors

- **`apache-proxy`**: Apache/proxy service logs (excludes cache-cleaner sidecars)
- **`api-service`**: Commerce API service logs
- **`backoffice`**: Backoffice administrative interface logs
- **`background-processing`**: Asynchronous task processing logs
- **`jsapps`**: JavaScript application logs
- **`imageprocessing`**: Media processing service logs
- **`zookeeper`**: Zookeeper coordination service logs

### Selector Implementation

```go
"apache-proxy": {
    Name:        "apache-proxy",
    DisplayName: "Apache Proxy Service",
    AzurePrefix: "kubernetes/",
    Predicate: func(blobName string) bool {
        return strings.Contains(blobName, "apache2-igc") &&
               strings.Contains(blobName, "_proxy-") &&
               !strings.Contains(blobName, "_cache-cleaner-")
    },
}
```

## Event Types

### BlobObserved Event

Published for each discovered blob:

```json
{
  "subscription": "cp2",
  "environment": "D1", 
  "blobName": "kubernetes/20250607.zookeeper-0_default_zookeeper-abc123.gz",
  "observationDate": "2025-06-07T18:43:22Z",
  "sizeInBytes": 2140870,
  "lastModifiedDate": "2025-06-07T21:42:54Z",
  "serviceSelector": "zookeeper"
}
```

### BlobsListed Event

Published after completing a scan for a specific date/service/environment:

```json
{
  "subscription": "cp2",
  "environment": "D1",
  "serviceSelector": "zookeeper",
  "date": "20250607",
  "listingStartTime": "2025-06-07T18:43:22Z",
  "listingEndTime": "2025-06-07T18:43:25Z", 
  "blobCount": 15,
  "totalBytes": 31457280
}
```

### BlobClosed Event

Published when a blob is considered closed (no new data expected):

```json
{
  "subscription": "cp2",
  "environment": "D1",
  "blobName": "kubernetes/20250607.zookeeper-0_default_zookeeper-abc123.gz",
  "serviceSelector": "zookeeper",
  "lastModifiedDate": "2025-06-07T21:42:54Z",
  "closedDate": "2025-06-07T21:47:54Z",
  "sizeInBytes": 2140870,
  "timeoutMinutes": 5
}
```

This event signals to downstream workers that:
- The blob has not been modified for the configured timeout period
- No new data is expected to be written to this blob
- Workers can stop polling the blob for changes
- The blob is ready for final processing

## Usage

### Running Locally

```bash
# Build and run with default config
make run

# Or build first, then run manually
make build
./bin/blob-monitor configs/config.yaml
```

### Docker Deployment

```bash
# Build and run with Docker (using Makefile)
make docker-run

# Or manually
docker build -f deployments/Dockerfile -t commerce-blob-monitor .
docker run -v $(pwd)/configs:/app/configs commerce-blob-monitor configs/config.yaml
```

### Environment Variables

The service uses the same Azure storage configuration as other pipeline components:
- Reads from `~/.commerce-logs-pipeline/config.yaml`
- Supports subscription/environment-based storage account mapping

## Monitoring and Operations

### Logging

The service provides comprehensive logging:

```
🚀 Starting Commerce Cloud Blob Monitor Service
📡 Kafka: localhost:9092 -> Ingestion.Blobs
🌍 Monitoring 3 environments
🔐 Starting blob closing detection (timeout: 5 minutes)
🔍 Starting historical blob scan...
📅 Scanning from 2025-06-04 to 2025-06-06
📅 Scanning date: 20250607
🔍 Scanning cp2/D1 for zookeeper on 20250607
📦 Found 5 zookeeper blobs for cp2/D1 on 20250607 (2.04 MB)
📡 Started monitoring cp2/D1 (interval: 10m0s)
```

### Key Metrics to Monitor

- **Blob Discovery Rate**: Number of new blobs found per scan
- **Scan Duration**: Time taken to complete environment scans
- **Kafka Delivery Success**: Event publishing confirmation rate
- **Error Rate**: Failed Azure API calls or Kafka deliveries
- **Environment Coverage**: Ensure all environments are being scanned

### Error Scenarios

1. **Azure Access Issues**: Check storage account credentials and permissions
2. **Kafka Connectivity**: Verify broker connectivity and topic existence
3. **Selector Errors**: Config validation will catch unknown selectors at startup
4. **Date Range Issues**: Invalid date formats or negative ranges

## Operational Behavior

### Historical Scan

On startup, the service:
1. Calculates start date based on `days_back` or `specific_date`
2. Scans each date from start to yesterday
3. For each date, scans all environments and selectors
4. Publishes BlobObserved events for found blobs
5. Publishes BlobsListed completion events

### Current Day Monitoring

After historical scan:
1. Starts periodic polling for each environment
2. Uses environment-specific polling intervals
3. Always scans current day
4. During EOD overlap period, also scans previous day
5. Continues until service is stopped

### End-of-Day Overlap Handling

During the first hour of each day (configurable):
- Scans both current and previous day
- Handles delayed blob rotation or clock differences
- Ensures no blobs are missed during day transitions

## Integration Points

### Upstream Dependencies
- **Azure Blob Storage**: Source of log files
- **Commerce Cloud**: Generates log files

### Downstream Consumers
- **Log Ingestion Service**: Consumes BlobObserved events to trigger ingestion
- **Monitoring Systems**: Track blob discovery metrics
- **Data Pipeline**: Use BlobsListed events for completion tracking

## Development

### Adding New Selectors

1. Add selector definition to `internal/selectors/selectors.go`:
```go
"new-service": {
    Name:          "new-service",
    DisplayName:   "New Service",
    Description:   "New service logs",
    AzurePrefix:   "kubernetes/",
    ServicePrefix: ".new-service",
    Predicate: func(blobName string) bool {
        return strings.Contains(blobName, "new-service") &&
               !strings.Contains(blobName, "_cache-cleaner-")
    },
}
```

2. Add tests in `internal/selectors/selectors_test.go`:
```go
func TestNewServiceSelector(t *testing.T) {
    selector, err := GetSelector("new-service")
    require.NoError(t, err)
    // Add test cases...
}
```

3. Reference in configuration:
```yaml
environments:
  - selectors:
      - "new-service"
```

### Testing

```bash
# Run all tests
make test

# Run only unit tests (fast)
make test-unit

# Run only integration tests (requires Docker)
make test-integration

# Run tests with coverage report
make test-coverage

# Manual testing
go test ./internal/...                    # Unit tests
go test -tags=integration ./test/...      # Integration tests
```

### Building

```bash
# Build using Makefile (recommended)
make build

# Development build with race detection
make dev-build

# Cross-platform release builds
make release

# Build Docker image
make docker-build

# Manual build (from cmd directory)
cd cmd/blob-monitor && go build -o ../../bin/blob-monitor .
```

## Configuration Reference

### Complete Configuration Options

```yaml
kafka:
  brokers: string              # Kafka broker addresses
  topic: string                # Target topic for events
  producer_config:             # Additional Kafka producer settings
    acks: string
    retries: int
    retry_backoff_ms: int

global:
  polling_interval: int        # Default polling interval (seconds)
  eod_overlap_minutes: int     # End-of-day overlap period (minutes)
  timezone: string             # Timezone for date calculations
  blob_closing:
    enabled: bool              # Enable blob closing detection
    timeout_minutes: int       # Minutes without modification to consider blob closed

date_range:
  specific_date: string        # Start from specific date (YYYY-MM-DD)
  days_back: int              # OR look back N days
  monitor_current_day: bool    # Continue monitoring current day

environments:
  - subscription: string       # Azure subscription ID
    environment: string        # Environment code (D1, S1, P1, etc.)
    enabled: bool             # Enable monitoring for this environment
    polling_interval: int      # Override global polling interval
    selectors: []string        # List of selector names to monitor

storage:
  container_name: string       # Azure storage container name

monitoring:
  enable_metrics: bool         # Enable metrics collection
  log_level: string           # Log level (debug, info, warn, error)
  stats_report_interval: int   # Report stats every N iterations

error_handling:
  max_azure_retries: int       # Maximum retries for Azure API calls
  retry_strategy: string       # Retry strategy (exponential, linear, fixed)
  max_retry_delay: int        # Maximum delay between retries (seconds)
  continue_on_environment_error: bool  # Continue if one environment fails
``` 