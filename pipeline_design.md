# Log-Derived Trace Pipeline Design

## Overview

This document describes the design of the data processing pipeline that produces OpenTelemetry-compatible traces from logs collected across SAP Commerce services. The pipeline is designed as a modular, stream-oriented system built on Kafka and OpenSearch, capable of handling incomplete, out-of-order data, and supporting append-only semantics.

## Key Principles

* **Append-only span events**: Each service emits immutable span documents with only the fields it knows.
* **No in-place mutation**: Span enrichments are modeled as new span events that will be merged downstream.
* **Trace assembly deferred**: Trace IDs and parent relationships are resolved by a dedicated trace assembly service.
* **Parallel linking**: Independent correlators operate in parallel to incrementally enrich the span stream.

---

## Pipeline Components

### 1. Ingestion

* Reads logs from Azure Blob Storage
* Emits raw log lines to Kafka topics:

  * `Raw.ProxyLogs`
  * `Raw.ApplicationLogs`

### 2. Extraction Services

* Parse and normalize logs into structured messages:

  * `Extracted.Proxy`
  * `Extracted.Application`
* Extracted logs are enriched with metadata like timestamps, service name, and container identifiers

#### Message Partitioning and Ordering

**Critical Design Requirement**: Log messages must maintain their original sequential ordering for accurate trace reconstruction and timeline analysis.

**Partitioning Strategy**:
- **Ingestion Phase**: Uses FNV32a hash of blob name to determine partition
  - `partition = fnv32a(blobName) % partitionCount`
  - Raw logs from the same blob are guaranteed to go to the same partition
  - Messages within a partition maintain strict ordering (line-1, line-2, line-3...)
- **Extraction Phase**: Replicates the exact same FNV32a hash algorithm
  - Extracts blob name from message key: `{subscription}:{environment}:{eventType}:{blobName}`
  - Calculates `partition = fnv32a(blobName) % 12` (same as ingestion)
  - **Critical**: Must NOT use Kafka's default key-based partitioning (different algorithm)
  - **Result**: Perfect ordering preservation across ingestion → extraction phases

**Message Key Format**: `{subscription}:{environment}:{eventType}:{blobName}`
- Example: `cp2:D1:line-42:20250615.api-695cfd5f68-jmhq5_default_platform-hash.gz`
- Blob name portion drives consistent partitioning across all pipeline phases

**Why This Matters**:
- Log sequence integrity is essential for request correlation
- Out-of-order processing would break thread-based and time-based correlations
- Maintaining partition consistency ensures downstream correlators can use local state efficiently

### 3. Linking / Correlation Services

Each correlator processes extracted logs and emits **partial span events** to the `Spans` topic:

* **App Request ↔ Message Correlation**

  * Links internal logs to known app-level request logs (by thread ID)
  * All necessary data resides within the same blob, enabling use of local embedded state (e.g., RocksDB)

* **App ↔ Proxy Requests Correlation**

  * Matches application-level HTTP request logs with upstream load balancer requests
  * Correlation based on user IP, internal proxy IP, and request metadata
  * Partitioning by internal proxy IP allows local state usage

* **Outbound Requests Extraction**

  * Detects and emits spans representing outbound API calls from application logs
  * Correlation logic is localized and suitable for local state

* **Background Processes Extraction**

  * Emits structured spans for job/task executions (e.g., cron jobs, batch imports)

All correlators are independent and emit enriched span events based on local inference, using embedded storage (e.g., RocksDB) for temporary correlation state.

### 4. Span Events (Append-Only)

* Unified Kafka topic (e.g., `SpanEvents`)
* Each message represents a partial or complete span:

```json
{
  "span_id": "abc123",
  "trace_id": null,
  "parent_span_id": "xyz456",
  "is_root": false,
  "start_time_unix_nano": ...,
  "end_time_unix_nano": ...,
  "attributes": { ... }
}
```

* Fields can be missing; downstream logic will join them

### 5. Trace Assembly

* Consumes `SpanEvents`
* Maintains span state using a shared, queryable, persistent store (e.g., PostgreSQL or ScyllaDB)
* This enables assembling spans from multiple services that may contribute to the same trace and are not easily co-partitioned
* Joins span events by `span_id`
* Resolves:

  * `trace_id`
  * `parent_span_id`
  * complete attribute set
* Emits final span document to OpenSearch `Traces Index`
* Deduplicates via `_id = trace_id + span_id`

---

## OpenSearch Ingestion

* **Final span documents only** are written
* Schema and index strategy are defined in the separate Trace Model Specification
* `Logs Index` stores raw logs in parallel, supporting cross-querying by `message.id`
