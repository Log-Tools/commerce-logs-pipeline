# Pipeline Events

This package defines the event structures and Kafka topics used throughout the commerce logs pipeline.

## Event Types

### Blob Lifecycle Events

#### BlobObservedEvent
Published when a blob is discovered by either the blob-monitor or ingest pipeline.
- **Topic**: `Ingestion.Blobs`
- **Key Format**: `{subscription}:{environment}:observed:{cleanBlobName}`

#### BlobsListedEvent
Published when blob-monitor completes scanning a specific date/service/environment.
- **Topic**: `Ingestion.Blobs`
- **Key Format**: `{subscription}:{environment}:listed:{serviceSelector}-{date}`

#### BlobClosedEvent
Published when blob-monitor determines a blob is closed (inactive beyond timeout).
- **Topic**: `Ingestion.Blobs`
- **Key Format**: `{subscription}:{environment}:closed:{cleanBlobName}`

#### BlobCompletionEvent
Published when ingest pipeline completes processing a blob segment.
- **Topic**: `Ingestion.Blobs`
- **Key Format**: `{subscription}:{environment}:offset{fromOffset}-completion:{cleanBlobName}`

#### BlobStateEvent
Canonical blob state maintained in compacted topic. Derived from all other blob events.
- **Topic**: `Ingestion.BlobState`
- **Key Format**: `{subscription}:{environment}:{blobName}`

## Kafka Topics

### Ingestion.Blobs
Contains all blob lifecycle events. Used as source of truth for blob discovery and processing status.

**Configuration**:
```properties
log.cleanup.policy=delete
log.retention.hours=168  # 7 days
log.segment.hours=24
```

### Ingestion.RawLogs
Contains actual log lines extracted from blobs.

**Configuration**:
```properties
log.cleanup.policy=delete
log.retention.hours=48   # 2 days (short retention for processing)
log.segment.hours=6
```

### Ingestion.State
Contains pipeline state and completion events.

**Configuration**:
```properties
log.cleanup.policy=delete
log.retention.hours=720  # 30 days
log.segment.hours=24
```

### Ingestion.BlobState
**COMPACTED TOPIC** - Contains current state of each blob.

**Required Configuration**:
```properties
log.cleanup.policy=compact
log.segment.hours=1          # Frequent compaction
log.min.cleanable.dirty.ratio=0.1  # Aggressive compaction
log.delete.delay.ms=60000    # 1 minute delay before deletion
min.compaction.lag.ms=0      # Immediate compaction eligibility
```

**Key Structure**: `{subscription}:{environment}:{blobName}`

**Purpose**: 
- Provides current state (open/closed) of each blob
- Tracks ingestion progress (bytes processed, lines ingested)
- Maintains blob metadata (size, last modified, service selector)
- Enables efficient blob state queries without scanning event history

## Event Processing Flow

```
[Blob Discovery] → BlobObservedEvent → [State Processor] → BlobStateEvent
[Blob Timeout]   → BlobClosedEvent   → [State Processor] → BlobStateEvent (closed)
[Blob Ingestion] → BlobCompletionEvent → [State Processor] → BlobStateEvent (progress)
```

The BlobStateProcessor consumes events from `Ingestion.Blobs` and maintains the canonical state in `Ingestion.BlobState`.

## Key Generation

All blob event keys use the shared `GenerateBlobEventKey()` function to ensure consistency:

```go
key := events.GenerateBlobEventKey(subscription, environment, eventType, blobName)
```

This ensures proper partitioning and enables efficient querying by blob identity. 