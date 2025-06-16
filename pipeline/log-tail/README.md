# Log Tail CLI

A powerful CLI utility for tailing logs from Kafka topics in the commerce logs pipeline with advanced filtering capabilities.

## Features

- 🚀 **Real-time log streaming** from multiple Kafka topics
- 🔍 **Advanced filtering** by subscription, environment, and service
- 📋 **Multiple output formats** (text, JSON) with smart log parsing
- 🎨 **Beautiful formatted output** with emojis and colors
- ⚙️ **Configurable** via YAML configuration files
- 🛡️ **Graceful shutdown** with proper signal handling
- 📊 **Multiple log types** support (HTTP requests, application logs, proxy logs)

## Installation

### Prerequisites

- Go 1.21 or later
- Access to Kafka cluster
- librdkafka (for Kafka client)

### Install librdkafka

#### Ubuntu/Debian
```bash
sudo apt-get install librdkafka-dev
```

#### macOS
```bash
brew install librdkafka
```

#### CentOS/RHEL
```bash
sudo yum install librdkafka-devel
```

### Build from Source

```bash
cd pipeline/log-tail
go build -o log-tail main.go
```

## Configuration

### Setup Configuration File

1. Create the configuration directory:
```bash
mkdir -p ~/.commerce-logs-pipeline
```

2. Copy the example configuration:
```bash
cp config.yaml.example ~/.commerce-logs-pipeline/config.yaml
```

3. Edit the configuration with your Kafka brokers and subscription details:
```bash
vim ~/.commerce-logs-pipeline/config.yaml
```

### Configuration Structure

```yaml
kafka:
  brokers: "localhost:9092"
  consumer_config:
    auto.offset.reset: "latest"
    enable.auto.commit: "true"

subscriptions:
  cp2:
    name: "Commerce Cloud Platform 2"
    environments:
      P1:
        name: "Production Environment 1"
        # ... storage account details
```

## Usage

### Basic Usage

```bash
# Tail all logs from all topics
./log-tail

# Tail logs with specific subscription and environment
./log-tail --subscription cp2 --environment P1

# Filter by service (e.g. api, backoffice, backgroundprocessing)
./log-tail --service api

# Specify custom topics
./log-tail --topics "Raw.ApplicationLogs,Extracted.Application"
```

### Advanced Options

```bash
# Output as JSON
./log-tail --format json

# Show raw message metadata
./log-tail --raw

# Use custom Kafka brokers
./log-tail --brokers "kafka1:9092,kafka2:9092"

# Use custom consumer group
./log-tail --consumer-group my-log-tail-group

# Custom config file
./log-tail --config /path/to/config.yaml
```

### Filtering Examples

```bash
# Production API logs only
./log-tail --subscription cp2 --environment P1 --service api

# All development environment logs
./log-tail --environment D1

# Proxy logs from specific subscription
./log-tail --subscription cp2 --topics "Raw.ProxyLogs,Extracted.Proxy"

# Error logs only
./log-tail --topics "Extraction.Errors"
```

## Available Topics

The CLI can consume from the following Kafka topics:

- **`Raw.ApplicationLogs`** - Raw application log lines from services
- **`Raw.ProxyLogs`** - Raw proxy/load balancer log lines
- **`Extracted.Application`** - Structured application logs
- **`Extracted.Proxy`** - Structured proxy logs
- **`Extraction.Errors`** - Log processing error events
- **`Ingestion.Blobs`** - Blob lifecycle events
- **`Ingestion.State`** - Pipeline state events

## Output Formats

### Text Format (Default)

The text format provides human-readable output with emojis and intelligent parsing:

```
🕐 14:32:15 | 🌐 Extracted.Application | GET /api/users → 200 (45ms) | 📍 192.168.1.100 | 🔧 api-pod-abc123
🕐 14:32:16 | 📝 Raw.ApplicationLogs | ℹ️ [INFO] User login successful | 🏷️ com.example.UserService | 🔧 api-pod-abc123
🕐 14:32:17 | 🔄 Extracted.Proxy | POST /api/auth → 401 (12ms) | 📍 10.0.0.50 | 🖥️ proxy-server-01
```

### JSON Format

The JSON format provides structured output for programmatic processing:

```json
{
  "method": "GET",
  "path": "/api/users",
  "status_code": 200,
  "response_time_ms": 45,
  "client_ip": "192.168.1.100",
  "pod_name": "api-pod-abc123"
}
```

### Raw Format

Shows complete Kafka message metadata:

```
🕐 14:32:15.123 | 📝 Extracted.Application[0]@12345 | 🔑 cp2:P1:extracted:api-logs.gz
{"method":"GET","path":"/api/users"...}
---
```

## Command Line Options

| Flag | Short | Description | Default |
|------|-------|-------------|---------|
| `--config` | `-c` | Configuration file path | Auto-detected |
| `--subscription` | `-s` | Subscription filter | All subscriptions |
| `--environment` | `-e` | Environment filter | All environments |
| `--service` | | Service filter | All services |
| `--topics` | `-t` | Specific topics to consume | All log topics |
| `--format` | `-f` | Output format (text/json) | `text` |
| `--raw` | | Show raw message metadata | `false` |
| `--consumer-group` | `-g` | Kafka consumer group ID | `log-tail-cli` |
| `--brokers` | | Kafka brokers (overrides config) | From config |

## Service Filters

The following service names are commonly available:

- **`api`** - API service logs
- **`backoffice`** - Back office service logs  
- **`backgroundprocessing`** - Background processing service logs
- **`proxy`** - Proxy/load balancer logs

Service filtering works by checking:
1. Kubernetes pod names for service indicators
2. Structured log metadata (in extracted logs)
3. Fallback string matching in log content

## Signal Handling

The CLI supports graceful shutdown:

- **Ctrl+C (SIGINT)** - Graceful shutdown
- **SIGTERM** - Graceful shutdown

## Troubleshooting

### Common Issues

1. **Kafka Connection Failed**
   ```bash
   # Check if Kafka brokers are accessible
   telnet localhost 9092
   ```

2. **No Configuration Found**
   ```bash
   # Verify config file location
   ls -la ~/.commerce-logs-pipeline/config.yaml
   ```

3. **Permission Denied**
   ```bash
   # Check config file permissions
   chmod 600 ~/.commerce-logs-pipeline/config.yaml
   ```

4. **librdkafka Missing**
   ```bash
   # Install librdkafka development libraries
   sudo apt-get install librdkafka-dev
   ```

### Debug Mode

Enable verbose logging by setting environment variable:
```bash
export KAFKA_LOG_LEVEL=debug
./log-tail --subscription cp2
```

## Development

### Build for Development

```bash
go mod tidy
go build -o log-tail main.go
```

### Run Tests

```bash
go test ./...
```

### Cross Compilation

```bash
# Linux
GOOS=linux GOARCH=amd64 go build -o log-tail-linux main.go

# macOS  
GOOS=darwin GOARCH=amd64 go build -o log-tail-macos main.go

# Windows
GOOS=windows GOARCH=amd64 go build -o log-tail.exe main.go
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is part of the commerce-logs-pipeline and follows the same license terms. 