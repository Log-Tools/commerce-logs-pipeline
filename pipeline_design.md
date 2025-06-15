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
