# Ingestion Service

The ingestion service handles the processing of raw log data from Azure Blob Storage into Kafka. It supports both CLI mode (for backwards compatibility and dev/test) and worker mode (for production scalable deployments).

## Overview

The service processes log files from Azure Blob Storage, extracts log lines, and publishes them to Kafka topics. It supports:

- **CLI Mode**: Direct blob processing (backwards compatible)
- **Worker Mode**: Continuous monitoring and processing with filtering and sharding
- **Filtering**: By date, subscription, environment, and selector patterns
- **Sharding**: Horizontal scaling across multiple worker instances
- **Resumable Processing**: Offset-based processing for reliability

## Architecture

```
Azure Blob Storage → Ingestion Service → Kafka Topics
                          ↑
                    Blob State Topic
                    (Worker Mode)
```

### Modes

#### CLI Mode
- Direct blob processing from command line
- Backwards compatible with original ingest tool
- Suitable for dev/test and one-off processing

#### Worker Mode
- Continuous monitoring of blob state topic
- Automatic processing of new/changed blobs
- Supports filtering and sharding for production use
- Scalable across multiple instances

## Configuration

### Environment Variables (CLI Mode)

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `INGEST_MODE` | Mode: 'cli' or 'worker' | No | `worker` |
| `SUBSCRIPTION_ID` | Azure subscription ID | Yes (CLI) | - |
| `ENVIRONMENT` | Environment name | Yes (CLI) | - |
| `AZURE_STORAGE_CONTAINER_NAME` | Container name | Yes (CLI) | - |
| `AZURE_STORAGE_BLOB_NAME` | Blob name to process | Yes (CLI) | - |
| `START_OFFSET` | Byte offset to start from | No | `0` |
| `KAFKA_BROKERS` | Kafka broker addresses | Yes | `localhost:9092` |
| `AZURE_STORAGE_ACCOUNT_NAME` | Storage account name | Yes | - |
| `AZURE_STORAGE_ACCESS_KEY` | Storage account key | Yes | - |

### Environment Variables (Worker Mode)

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `BLOB_STATE_TOPIC` | Blob state topic name | No | `Ingestion.BlobState` |
| `CONSUMER_GROUP` | Kafka consumer group | No | `ingestion-worker` |
| `SHARDING_ENABLED` | Enable sharding | No | `false` |
| `SHARDS_COUNT` | Total number of shards | No | `1` |
| `SHARD_NUMBER` | This worker's shard number | No | `0` |
| `MIN_DATE` | Minimum blob date filter | No | - |
| `MAX_DATE` | Maximum blob date filter | No | - |
| `SUBSCRIPTIONS` | Comma-separated subscriptions | No | - |
| `ENVIRONMENTS` | Comma-separated environments | No | - |
| `SELECTORS` | Comma-separated selector patterns | No | - |

### Configuration Files

You can also use YAML configuration files:

#### CLI Mode Example (`configs/cli-example.yaml`)
```yaml
mode: cli
cli:
  subscription_id: "cp2"
  environment: "P1"
  container_name: "logs"
  blob_name: "app-service.log.gz"
  start_offset: 0
kafka:
  brokers: "localhost:9092"
storage:
  account_name: "${AZURE_STORAGE_ACCOUNT_NAME}"
  access_key: "${AZURE_STORAGE_ACCESS_KEY}"
```

#### Worker Mode Example (`configs/worker-example.yaml`)
```yaml
mode: worker
worker:
  blob_state_topic: "Ingestion.BlobState"
  consumer_group: "ingestion-worker-shard-0"
  filters:
    min_date: "2024-01-01T00:00:00Z"
    subscriptions: ["cp2", "cp3"]
    environments: ["P1", "S1"]
    selectors: ["apache-.*", "app-.*"]
  sharding:
    enabled: true
    shards_count: 4
    shard_number: 0
  processing:
    loop_interval: "30s"
    max_concurrent_blobs: 5
```

## Usage

### CLI Mode (Backwards Compatibility)

```bash
# Using environment variables
export INGEST_MODE=cli
export SUBSCRIPTION_ID=cp2
export ENVIRONMENT=P1
export AZURE_STORAGE_CONTAINER_NAME=logs
export AZURE_STORAGE_BLOB_NAME=app.log.gz
export START_OFFSET=0
./bin/ingest

# Using command line flags
./bin/ingest -mode=cli

# Using configuration file
./bin/ingest -config=configs/cli-example.yaml
```

### Worker Mode (Production)

```bash
# Single worker processing all blobs
export INGEST_MODE=worker
./bin/ingest

# Sharded workers (run multiple instances)
# Worker 1 (shard 0 of 4)
export SHARDING_ENABLED=true
export SHARDS_COUNT=4
export SHARD_NUMBER=0
export CONSUMER_GROUP=ingestion-worker-shard-0
./bin/ingest

# Worker 2 (shard 1 of 4)
export SHARD_NUMBER=1
export CONSUMER_GROUP=ingestion-worker-shard-1
./bin/ingest

# Using configuration file
./bin/ingest -config=configs/worker-example.yaml
```

### Testing

```bash
# Test Kafka connectivity
./bin/ingest -test-kafka

# Test with specific brokers
./bin/ingest -test-kafka -brokers="broker1:9092,broker2:9092"
```

## Worker Mode Features

### Filtering

Workers can filter blobs based on:

- **Date Range**: Process only blobs within specified date range
- **Subscriptions**: Process only specific Azure subscriptions
- **Environments**: Process only specific environments (P1, S1, etc.)
- **Selectors**: Process only blobs matching regex patterns

### Sharding

Enable horizontal scaling by:

1. Set `sharding.enabled: true`
2. Configure `shards_count` (total workers)
3. Set unique `shard_number` for each worker (0-based)
4. Use different `consumer_group` for each worker

Each worker processes only blobs where `hash(blobName) % shardsCount == shardNumber`.

### Blob State Processing

Workers:
1. Consume blob state events from compacted topic
2. Apply filters to determine relevant blobs
3. Track ingestion progress per blob
4. Process new/changed blobs automatically
5. Remove fully processed closed blobs from monitoring

## Building and Deployment

### Development

```bash
# Build
make build

# Run tests
make test

# Run in CLI mode
make run-cli

# Run in worker mode  
make run-worker

# Format and lint
make fmt lint
```

### Docker

```bash
# Build image
make docker-build

# Run CLI mode
make docker-run-cli

# Run worker mode
make docker-run-worker
```

### Production Deployment

```yaml
# Kubernetes deployment example
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ingestion-worker
spec:
  replicas: 4  # Number of shards
  template:
    spec:
      containers:
      - name: ingest
        image: commerce-logs-ingest:latest
        env:
        - name: INGEST_MODE
          value: "worker"
        - name: SHARDING_ENABLED
          value: "true"
        - name: SHARDS_COUNT
          value: "4"
        - name: SHARD_NUMBER
          valueFrom:
            fieldRef:
              fieldPath: metadata.annotations['shard-number']
```

## Monitoring

### Metrics to Monitor

- **Blob Processing Rate**: Blobs processed per minute
- **Line Processing Rate**: Log lines ingested per second
- **Kafka Delivery Success**: Message delivery confirmations
- **Worker Health**: Individual shard status
- **Lag Metrics**: Time between blob update and ingestion

### Log Output

The service provides structured logging:

```
🚀 Starting ingestion worker (shard 0/4)
📡 Starting blob state consumer
🔄 Processing blob app.log.gz from offset 1024
✅ Successfully processed blob app.log.gz
```

## Troubleshooting

### Common Issues

1. **Azure Storage Access**: Verify credentials and container permissions
2. **Kafka Connectivity**: Use `make test-kafka` to verify connection
3. **Blob State Topic**: Ensure topic exists and is properly compacted
4. **Sharding Conflicts**: Verify unique shard numbers and consumer groups

### Recovery

- **Offset Reset**: Consumer will resume from last committed offset
- **Failed Processing**: Check logs for specific blob processing errors
- **Scaling**: Add/remove workers by adjusting replica count

## Integration

### Upstream
- **Blob Monitor**: Publishes blob state events
- **Azure Storage**: Source of log files

### Downstream
- **Log Processing Pipeline**: Consumes from `Raw.ProxyLogs` and `Raw.ApplicationLogs`
- **Monitoring Systems**: Monitor blob completion events 