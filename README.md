# Commerce Logs Processing Pipeline

A scalable, multi-phase log ingestion and processing pipeline for commerce cloud environments.

## Architecture

This repository contains Go modules for pipeline phases and Python CLI tools for administration:

### Pipeline Flow

```
Azure Blob Storage → Blob Monitor → Kafka → Ingestion Pipeline → Processed Logs
                         ↓             ↓
                   [BlobObserved]  [BlobClosed]
                   [BlobsListed]      Events
```

1. **Blob Monitor** continuously scans Azure Blob Storage and publishes discovery events
2. **Ingestion Pipeline** consumes blob events and processes the actual log data
3. **Blob Closing Detection** signals when blobs are ready for final processing

```
├── pipeline/                    # Go modules for pipeline phases
│   ├── blob-monitor/           # Azure Blob discovery and monitoring
│   ├── ingest/                 # Azure Blob → Kafka ingestion 
│   ├── log-tail/               # Real-time log tailing CLI
│   └── config/                 # Shared configuration library
├── cli/                        # Python CLI tools
│   ├── src/                    # Python modules (config, azure_client)
│   ├── scripts/                # CLI scripts
│   └── config/                 # Config templates
└── go.work                     # Go workspace configuration
```

## Modules Overview

### ✅ Blob Monitor (`pipeline/blob-monitor`)
- Continuously monitors Azure Blob Storage for new log files
- Publishes blob discovery events to Kafka for downstream processing
- **Blob Closing Detection**: Automatically detects when blobs are no longer being written to
- Multi-environment monitoring with configurable selectors
- End-of-day overlap handling for timezone transitions

### ✅ Ingestion (`pipeline/ingest`)
- Downloads gzipped log segments from Azure Blob Storage
- Streams lines to Kafka topics with completion tracking
- Handles resumable processing with byte-level offsets

### ✅ Log Tail CLI (`pipeline/log-tail`)
- Real-time Kafka log consumption with advanced filtering
- Filter by subscription, environment, and service selector
- Multiple output formats (text, JSON) with smart log parsing
- Support for all pipeline topics (Raw, Extracted, Errors)

### ✅ Configuration (`pipeline/config`)
- Shared configuration library for all pipeline phases
- YAML-based multi-environment configuration
- Secure credential handling

## Configuration

### Secure Configuration Setup

The pipeline uses a YAML configuration format for managing multiple environments:

```yaml
subscriptions:
  cp2:
            name: "Commerce Cloud Subscription"
    environments:
      P1:
        name: "Production Environment 1"
        storage_account:
          account_name: "prod_storage_account"
          access_key: "your_access_key_here"
```

**Setup secure configuration:**
```bash
cd cli/
python scripts/setup_secure_config.py
```

This creates a secure config at `~/.commerce-logs-pipeline/config.yaml` with proper permissions.

## Quick Start

### Prerequisites
- Go 1.21+
- Python 3.9+
- Access to Azure Blob Storage
- Kafka cluster

### 1. Install Go Dependencies
```bash
go work sync
cd pipeline/ingest && go mod tidy
cd ../config && go mod tidy
```

### 2. Install Python CLI Tools

#### Option A: Using Virtual Environment (Recommended)
```bash
cd cli/

# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Install the CLI tools in development mode
pip install -e .
```

#### Option B: Using pipx (Alternative)
```bash
cd cli/
pipx install -e .
```

**Note:** On modern Ubuntu/Debian systems, you may encounter an "externally-managed-environment" error when using pip directly. The virtual environment approach (Option A) is recommended for development.

#### Activating the Virtual Environment
After initial setup, you'll need to activate the virtual environment each time you want to use the CLI tools:

```bash
cd cli/
source venv/bin/activate

# Now you can use the CLI commands
setup-config
list-blobs --help

# When done, deactivate the environment
deactivate
```

### 3. Configure Credentials
```bash
setup-config
# Edit ~/.commerce-logs-pipeline/config.yaml with your credentials
```

### 4. Test Configuration
```bash
list-blobs --env P1
```

## CLI Tools

### Available Commands

#### Python CLI Tools (in `cli/`)
| Command | Description |
|---------|-------------|
| `setup-config` | Set up secure configuration |
| `list-blobs` | List blobs from Azure storage |
| `list-services` | List available services (planned) |
| `analyze-logs` | Analyze log content (planned) |

#### Go CLI Tools 
| Command | Location | Description |
|---------|----------|-------------|
| `log-tail` | `pipeline/log-tail/` | Real-time log tailing from Kafka topics |

### Examples

#### Python CLI Examples
```bash
# List all blobs
list-blobs

# List blobs for specific environment
list-blobs --env P1

# Filter by date and service
list-blobs --date 20250601 --service backgroundprocessing

# Limit results
list-blobs --max-per-env 10
```

#### Log Tail CLI Examples
```bash
# Build and run log-tail CLI
cd pipeline/log-tail
make build-local

# Tail all logs
./log-tail

# Filter by subscription and environment
./log-tail --subscription cp2 --environment P1

# Filter by service
./log-tail --service api

# JSON output with raw metadata
./log-tail --format json --raw

# Specific topics only
./log-tail --topics "Raw.ApplicationLogs,Extracted.Application"
```

## Development

### Development Standards

📖 **For all development work, please read [DEVELOPMENT.md](DEVELOPMENT.md) first.**

This document defines:
- Standard Go project structure for all modules
- Testing strategies (unit vs integration)
- Makefile standards and required targets
- Code organization and naming conventions
- Documentation requirements

All new modules must follow these standards for consistency and maintainability.

### Go Modules

Currently implemented modules:

```bash
# Work on blob monitoring
cd pipeline/blob-monitor
go run cmd/blob-monitor/main.go configs/config.yaml

# Work on ingestion
cd pipeline/ingest
go run main.go

# Work on log tailing CLI
cd pipeline/log-tail
make run

# Work with configuration
cd pipeline/config
go test
```

### OpenSearch Deployment

A minimal OpenSearch setup is provided under `deployments/opensearch`. It includes a Docker compose file and an init container that uploads the trace index mapping from `configs/opensearch/traces_index_mapping.json`. Start it locally with:

```bash
docker compose -f deployments/opensearch/docker-compose.yml up
```

This brings up an OpenSearch node ready for pipeline experiments.


### Quick Development Workflow

```bash
# 1. Start development infrastructure
make dev-up

# 2. Run blob monitor (in terminal 1)
make run-blob-monitor

# 3. Monitor Kafka topics (in terminal 2) 
# Visit http://localhost:9000 for Kafdrop UI

# 4. Run ingestion pipeline (in terminal 3)
cd pipeline/ingest
SUBSCRIPTION_ID=cp2 ENVIRONMENT=P1 \
AZURE_STORAGE_CONTAINER_NAME=commerce-logs-separated \
AZURE_STORAGE_BLOB_NAME=your-blob.gz \
KAFKA_BROKERS=localhost:9092 \
KAFKA_TOPIC=commerce-logs \
go run .

# 5. Stop everything
make dev-down
```

### Python Development

```bash
cd cli/

# If using virtual environment, activate it first
source venv/bin/activate

# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/
```

### Configuration in Go

```go
import "pipeline/config"

cfg, err := config.LoadConfig()
if err != nil {
    log.Fatal(err)
}

storage, err := cfg.GetStorageAccount("cp2", "P1")
if err != nil {
    log.Fatal(err)
}
```

## Environment Variables

### Ingestion Pipeline
- `SUBSCRIPTION_ID` - Azure subscription ID from config
- `ENVIRONMENT` - Environment name (P1, D1, S1)
- `AZURE_STORAGE_CONTAINER_NAME` - Container name
- `AZURE_STORAGE_BLOB_NAME` - Blob to process
- `KAFKA_BROKERS` - Comma-separated Kafka brokers
- `KAFKA_TOPIC` - Output topic
- `START_OFFSET` - Optional starting byte offset

## Running the Blob Monitor

### Prerequisites
- Secure configuration set up (`~/.commerce-logs-pipeline/config.yaml`)
- Running Kafka cluster
- Access to Azure Blob Storage

### Basic Usage

```bash
cd pipeline/blob-monitor

# Run with example configuration
go run cmd/blob-monitor/main.go configs/config.yaml

# Run with custom configuration
go run cmd/blob-monitor/main.go /path/to/your-config.yaml
```

### Configuration Example

```yaml
kafka:
  brokers: "localhost:9092"
  topic: "Ingestion.Blobs"

global:
  polling_interval: 300  # 5 minutes
  timezone: "UTC"
  blob_closing:
    enabled: true
    timeout_minutes: 5  # Consider blob closed after 5 minutes of inactivity

environments:
  - subscription: "cp2"
    environment: "D1"
    enabled: true
    selectors:
      - "apache-proxy"
      - "api-service"
      - "zookeeper"
```

### Event Types Published

The blob monitor publishes three types of events to Kafka:

1. **BlobObserved** - When a new blob is discovered
2. **BlobsListed** - When scanning completes for a date/service/environment
3. **BlobClosed** - When a blob is inactive for the configured timeout (enables workers to stop polling)

### Development and Testing

```bash
cd pipeline/blob-monitor

# Run tests
make test

# Build binary
make build

# Run with Docker
make docker-run
```

## Running the Ingestion Pipeline

### Prerequisites
- Secure configuration set up (`~/.commerce-logs-pipeline/config.yaml`)
- Running Kafka cluster
- Access to Azure Blob Storage

### Basic Usage

```bash
cd pipeline/ingest

# Set required environment variables
export SUBSCRIPTION_ID=cp2
export ENVIRONMENT=P1
export AZURE_STORAGE_CONTAINER_NAME=commerce-logs-separated
export AZURE_STORAGE_BLOB_NAME=kubernetes/20250606.apache2-igc-674b9bdb88-vm775_default_proxy-931d71bdaa7b0a1b8b623ff1df112725c6b1d36789518820d232868fc02ee46e.gz
export KAFKA_BROKERS=localhost:9092
export KAFKA_TOPIC=commerce-logs

# Optional: Set starting offset for resumable processing
export START_OFFSET=1048576  # Start from 1MB offset

# Run the ingestion
go run .
```

### One-liner Execution

```bash
cd pipeline/ingest

SUBSCRIPTION_ID=cp2 \
ENVIRONMENT=P1 \
AZURE_STORAGE_CONTAINER_NAME=commerce-logs-separated \
AZURE_STORAGE_BLOB_NAME=kubernetes/20250606.apache2-igc-674b9bdb88-vm775_default_proxy-931d71bdaa7b0a1b8b623ff1df112725c6b1d36789518820d232868fc02ee46e.gz \
KAFKA_BROKERS=localhost:9092 \
KAFKA_TOPIC=commerce-logs \
go run .
```

### Testing Connectivity

Before running ingestion, test Kafka connectivity:

```bash
cd pipeline/ingest

# Test default Kafka (localhost:9092)
go run . test-kafka

# Test custom Kafka brokers
go run . test-kafka "broker1:9092,broker2:9092"
```

### Process Flow

The ingestion pipeline performs these steps:

1. **Environment Validation** - Checks all required environment variables
2. **Configuration Loading** - Loads Azure credentials from secure config
3. **Kafka Connectivity Test** - Validates Kafka connection (fail-fast)
4. **Azure Authentication** - Authenticates with Azure Blob Storage
5. **Blob Processing** - Downloads, decompresses, and streams log lines
6. **Kafka Production** - Sends log lines to Kafka with completion tracking
7. **Completion Message** - Sends processing completion event to Kafka

### Resumable Processing

The pipeline supports resumable processing using byte offsets:

```bash
# Start from beginning (default)
START_OFFSET=0 go run .

# Resume from specific byte offset
START_OFFSET=2097152 go run .  # Resume from 2MB

# Get blob size first (using CLI tools)
list-blobs --env P1 | grep my-blob.gz
```

### Output Examples

**Successful execution:**
```
2025/06/06 07:30:15 🔍 Validating Kafka connectivity before processing...
2025/06/06 07:30:15 ✅ Kafka connectivity validated
2025/06/06 07:30:15 Starting processing for blob: commerce-logs-separated/my-blob.gz
2025/06/06 07:30:16 Successfully initiated download. Actual stream ContentLength: 40000000 bytes.
2025/06/06 07:30:20 Produced 1000 lines from my-blob.gz (offset 0) to Kafka topic commerce-logs...
2025/06/06 07:30:25 ✅ Kafka producer flushed successfully. All messages for segment my-blob.gz (offset 0) sent.
2025/06/06 07:30:25 🎉 Blob to Kafka processing completed successfully.
```

**Kafka connectivity failure:**
```
2025/06/06 07:30:15 🔍 Validating Kafka connectivity before processing...
2025/06/06 07:30:20 ❌ Kafka connectivity check failed: connection failed after 5s: Local: Broker transport failure
```

## Security

- Configuration files are stored outside the workspace
- Credentials are masked in CLI output
- Secure file permissions (600) are enforced
- No credentials in source code or version control

## Troubleshooting

### Configuration Migration

If you have an existing configuration at `~/.cch-log-sync/config.yaml`, it needs to be migrated to the new location:

```bash
# Migrate existing config
mkdir -p ~/.commerce-logs-pipeline
cp ~/.cch-log-sync/config.yaml ~/.commerce-logs-pipeline/config.yaml
chmod 600 ~/.commerce-logs-pipeline/config.yaml

# Remove old directory (optional)
rm -rf ~/.cch-log-sync
```

### ModuleNotFoundError for CLI Commands

If you encounter `ModuleNotFoundError: No module named 'setup_config_cli'` or similar errors:

1. **Ensure you're using the virtual environment:**
   ```bash
   cd cli/
   source venv/bin/activate
   ```

2. **Reinstall the package in development mode:**
   ```bash
   pip install -e .
   ```

3. **Verify the commands are available:**
   ```bash
   which setup-config
   which list-blobs
   ```

The CLI tools should now work correctly with the proper package structure.