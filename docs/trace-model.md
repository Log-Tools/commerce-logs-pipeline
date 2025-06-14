# OpenTelemetry-Compatible Trace Model for Log-Derived Traces

This document defines the span and trace structures used by the Commerce Logs Pipeline.
It is based on the design shared in the project discussion and is reproduced here for
reference.

## Goals

- OpenTelemetry span compatibility
- Support for log-derived spans with rich parent–child relationships
- Efficient correlation of spans from different services and stages (e.g. LB → app → external API)
- High ingestion throughput
- Scalable storage with OpenSearch
- Future support for full-text and vector search

## Span Model

### Core OpenTelemetry Fields

Each span document includes:

- `trace_id` (string): 32-character hex, UUIDv5
- `span_id` (string): 16-character hex, UUIDv5 truncated
- `parent_span_id` (string): optional, 16-character hex
- `name` (string): span name (for example `POST /login`)
- `kind` (string): span kind, one of `INTERNAL`, `SERVER`, `CLIENT`, `PRODUCER`, `CONSUMER`
- `start_time_unix_nano` (int64): span start time in Unix nanoseconds
- `end_time_unix_nano` (int64): span end time in Unix nanoseconds
- `status` object describing the span outcome with `code` and optional `message`

### Attributes

Under the `attributes` object the following keys are used (dynamic keys are allowed):

- `span.type`: `session`, `request`, `message`, `external`, `job`, `task`, `container`
- `subscription.code`, `environment.code`
- `service.name`, `thread.name`
- `http.method`, `http.status_code`, `user.ip`
- `message.id`
- `log.level`, `log.message`
- `k8s.namespace`, `k8s.pod_id`, `k8s.container_id`
- `external.url`, `external.status_code`
- `job.name`, `task.name`, `stderr`

### Optional Fields

- `embedding_vector`: dense vector for semantic search (e.g. MiniLM, 384 dims)
- `resource`: `{ service.version, host.name }`
- `span.links`: for modeling causal but non-hierarchical relationships

## ID Generation

A deterministic UUIDv5 strategy is used so that reprocessed logs yield the same
identifier. `trace_id` is generated using a namespace combined with a
session/job/container key. `span_id` uses a message key and is truncated to 16 hex
characters. This allows ingestion to be idempotent and removes the need for Kafka
exactly-once semantics.

## OpenSearch Schema

The index mapping is stored at `configs/opensearch/traces_index_mapping.json`.
It is applied through the `opensearch-init` tool or via the Docker Compose setup.

### Index Mapping Example

```json
{
  "mappings": {
    "properties": {
      "trace_id": { "type": "keyword" },
      "span_id": { "type": "keyword" },
      "parent_span_id": { "type": "keyword" },
      "name": { "type": "text" },
      "kind": { "type": "keyword" },
      "start_time_unix_nano": { "type": "long" },
      "end_time_unix_nano": { "type": "long" },
      "status": {
        "properties": {
          "code": { "type": "keyword" },
          "message": { "type": "text" }
        }
      },
      "attributes": {
        "type": "object",
        "dynamic": true,
        "properties": {
          "span.type": { "type": "keyword" },
          "subscription.code": { "type": "keyword" },
          "environment.code": { "type": "keyword" },
          "service.name": { "type": "keyword" },
          "thread.name": { "type": "keyword" },
          "http.method": { "type": "keyword" },
          "http.status_code": { "type": "integer" },
          "user.ip": { "type": "ip" },
          "message.id": { "type": "keyword" },
          "log.level": { "type": "keyword" },
          "log.message": { "type": "text" },
          "k8s.namespace": { "type": "keyword" },
          "k8s.pod_id": { "type": "keyword" },
          "k8s.container_id": { "type": "keyword" },
          "external.url": { "type": "text" },
          "external.status_code": { "type": "integer" },
          "job.name": { "type": "keyword" },
          "task.name": { "type": "keyword" },
          "stderr": { "type": "boolean" }
        }
      },
      "embedding_vector": {
        "type": "dense_vector",
        "dims": 384,
        "index": true,
        "similarity": "cosine"
      },
      "resource": {
        "properties": {
          "service.version": { "type": "keyword" },
          "host.name": { "type": "keyword" }
        }
      }
    }
  },
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 1,
    "knn": true
  }
}
```

## Indexing Strategy

Spans are indexed in daily indices using the pattern
`traces-{subscription}-{env}-yyyy.MM.dd`. A single trace may span multiple
daily indices when its spans cross midnight boundaries.

Queries should select all indices matching the trace's dates, for example:

```json
GET traces-*-2025.06.*/_search
{
  "query": { "term": { "trace_id": "abc123" } }
}
```

Aliases per tenant/environment can simplify lookups.

## OpenSearch Deployment

A minimal deployment is provided under `deployments/opensearch`. It uses Docker
Compose to start an OpenSearch node and an init container that loads the index
mapping automatically. The helper binary `opensearch-init` can also upload the
mapping to an existing cluster.

Run the deployment with:

```bash
docker compose -f deployments/opensearch/docker-compose.yml up
```

This command starts OpenSearch and applies the index template defined in
`configs/opensearch/traces_index_mapping.json`.
