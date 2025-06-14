# Traces Package

This module defines the target trace model used by the final stage of the Commerce Logs Pipeline.
It provides OpenTelemetry-compatible structures that describe spans derived from raw log lines.
The package is purely declarative for now and will be used by future analytical
and storage components of the pipeline.

## Overview

Spans generated from log events will be correlated into traces and stored in
OpenSearch.  The model follows the OpenTelemetry specification and includes
additional attributes required for Commerce Cloud logs.

## Span Model

The `Span` struct represents a single OpenTelemetry span document. Core fields:

- `trace_id` and `span_id` using deterministic UUIDv5 generation
- `parent_span_id` to establish parent-child relationships
- `kind` enumerated as `INTERNAL`, `SERVER`, `CLIENT`, `PRODUCER`, `CONSUMER`
- `start_time_unix_nano` and `end_time_unix_nano` timestamps
- `status` object with `code` and optional `message`
- `attributes` for log-derived metadata (HTTP method, service name, etc.)
- Optional `embedding_vector` for semantic search support
- `resource` fields like service version or host name
- `span_links` for causal references to other spans

See `model.go` for full structure definitions.

## Index Mapping

The OpenSearch index mapping for spans is provided in
`configs/opensearch/traces_index_mapping.json`. This mapping defines all
fields and settings required for storing spans in OpenSearch.

## Trace Model

The `Trace` struct is a simple grouping of spans sharing the same `trace_id`.
Actual trace correlation logic will be implemented in a later module.

## Status

This package only contains data model definitions and has no runtime
implementation yet. It serves as a foundation for building the final
trace output stage and for developing upstream analytical phases.

For full details of the trace model design see [docs/trace-model.md](../../docs/trace-model.md).
