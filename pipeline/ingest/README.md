# Raw Log Ingestion Pipeline

This component handles the initial ingestion of raw log data from Azure Blob Storage into Kafka for further processing.

## Overview

The raw ingestion pipeline is the first stage of the commerce logs processing system. It reads log files from Azure Blob Storage, processes them line by line, and publishes each line as a separate message to a Kafka topic for downstream processing.

## Architecture

```
Azure Blob Storage → Raw Ingestion Service → Kafka Topic (Ingestion.RawLogs)
```

### Key Components

- **Blob Reader**: Downloads and processes log files from Azure Blob Storage
- **Line Parser**: Reads log files line by line with support for compressed (gzip) files
- **Kafka Producer**: Publishes each log line as a separate Kafka message
- **Progress Tracking**: Maintains offset tracking for resumable processing

## Configuration

### Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `SUBSCRIPTION_ID` | Azure subscription ID | Yes | - |
| `ENVIRONMENT` | Environment name (dev, staging, prod) | Yes | - |
| `AZURE_STORAGE_CONTAINER_NAME` | Azure Storage container name | Yes | - |
| `AZURE_STORAGE_BLOB_NAME` | Blob file name to process | Yes | - |
| `KAFKA_BROKERS` | Comma-separated list of Kafka brokers | Yes | - |
| `KAFKA_TOPIC` | Kafka topic name | No | `Ingestion.RawLogs` |
| `START_OFFSET` | Byte offset to start reading from | No | `0` |

### Example Configuration

```bash
export SUBSCRIPTION_ID="your-azure-subscription-id"
export ENVIRONMENT="production"
export AZURE_STORAGE_CONTAINER_NAME="logs"
export AZURE_STORAGE_BLOB_NAME="commerce-app.log.gz"
export KAFKA_BROKERS="kafka-broker1:9092,kafka-broker2:9092"
export KAFKA_TOPIC="Ingestion.RawLogs"
export START_OFFSET="0"
```

## Message Format

### Raw Log Messages

Each log line is published as a separate Kafka message with the following structure:

- **Key**: `{blobName}-{lineNumber}` (e.g., `commerce-app.log.gz-1`, `commerce-app.log.gz-2`)
- **Value**: Raw log line content (as string)
- **Topic**: `Ingestion.RawLogs`

### Completion Messages

After processing a blob segment, a completion message is sent with the following JSON structure:

```json
{
  "blobName": "commerce-app.log.gz",
  "processedFromOffset": 0,
  "processedToEndOffset": 1048576,
  "linesSent": 12543
}
```

- **Key**: `{blobName}-offset{startOffset}-completion`
- **Topic**: `Ingestion.RawLogs`

## Features

### Resumable Processing
- Supports offset-based processing to resume from a specific position
- Tracks blob size changes to handle growing log files
- Completion messages enable downstream consumers to track progress

### File Format Support
- **Compressed Files**: Automatic gzip decompression
- **Large Files**: Streaming processing with configurable buffer sizes
- **Line-by-Line**: Each log line becomes a separate Kafka message

### Error Handling
- Graceful handling of connection failures
- Retry logic for Kafka message delivery
- Comprehensive logging for debugging and monitoring

### Performance Features
- **Buffered Reading**: 1MB buffer for efficient file processing
- **Batch Progress Reporting**: Log progress every 1000 lines
- **Producer Flushing**: Ensures all messages are delivered before completion

## Usage

### Running the Service

```bash
# Build the service
go build -o ingest .

# Run with environment variables
./ingest
```

### Testing Kafka Connection

```bash
# Test connection to default localhost:9092
./ingest test-kafka

# Test connection to specific brokers
./ingest test-kafka "broker1:9092,broker2:9092"
```

### Docker Deployment

```bash
# Build Docker image
docker build -t commerce-logs-ingest .

# Run with environment file
docker run --env-file .env commerce-logs-ingest
```

## Monitoring

### Log Output

The service provides detailed logging including:

- **Connection Status**: Azure and Kafka connectivity
- **Processing Progress**: Lines processed and batch updates
- **Error Conditions**: Failed deliveries and connection issues
- **Completion Status**: Successful processing confirmations

### Key Metrics to Monitor

- **Lines Processed**: Total number of log lines ingested
- **Processing Rate**: Lines per second throughput
- **Kafka Delivery Success**: Message delivery confirmations
- **Error Rate**: Failed message deliveries
- **Blob Processing Time**: End-to-end processing duration

## Error Scenarios

### Common Issues

1. **Azure Storage Access**: Verify subscription ID and container permissions
2. **Kafka Connectivity**: Check broker addresses and network connectivity
3. **Large Files**: Monitor memory usage and adjust buffer sizes if needed
4. **Incomplete Processing**: Check completion messages and restart from last offset

### Recovery Procedures

1. **Restart from Offset**: Use `START_OFFSET` environment variable
2. **Retry Failed Messages**: Review Kafka producer logs for delivery failures
3. **Blob Size Changes**: Service automatically handles growing files

## Integration Points

### Upstream
- **Azure Blob Storage**: Source of raw log files
- **File Upload Process**: External process that creates/updates log files

### Downstream
- **Log Processing Pipeline**: Consumes from `Ingestion.RawLogs` topic
- **Data Transformation**: Next stage processes and enriches raw log lines
- **Analytics Systems**: May consume raw logs for immediate analysis

## Development

### Prerequisites
- Go 1.19+
- Access to Azure Storage account
- Kafka cluster (local or remote)

### Testing
```bash
# Run tests
go test ./...

# Test with local Kafka
export KAFKA_BROKERS="localhost:9092"
go run main.go test-kafka
```

### Building
```bash
# Local build
go build -o ingest .

# Cross-platform build
GOOS=linux GOARCH=amd64 go build -o ingest-linux .
``` 