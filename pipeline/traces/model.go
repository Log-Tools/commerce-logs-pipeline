package traces

// SpanKind represents the OpenTelemetry span kind.
// See https://opentelemetry.io/docs/specs/otel/trace/api/#spankind
// Values mirror the official specification.
type SpanKind string

const (
	SpanKindInternal SpanKind = "INTERNAL"
	SpanKindServer   SpanKind = "SERVER"
	SpanKindClient   SpanKind = "CLIENT"
	SpanKindProducer SpanKind = "PRODUCER"
	SpanKindConsumer SpanKind = "CONSUMER"
)

// SpanStatus represents the outcome status of a span.
type SpanStatus struct {
	Code    string `json:"code"`              // OK, ERROR, or UNSET
	Message string `json:"message,omitempty"` // Optional human readable message
}

// SpanLink models causal but non-hierarchical relationships between spans.
type SpanLink struct {
	TraceID    string                 `json:"trace_id"`
	SpanID     string                 `json:"span_id"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
}

// Span represents a single OpenTelemetry-compatible span document.
type Span struct {
	TraceID           string                 `json:"trace_id"`
	SpanID            string                 `json:"span_id"`
	ParentSpanID      string                 `json:"parent_span_id,omitempty"`
	Name              string                 `json:"name"`
	Kind              SpanKind               `json:"kind"`
	StartTimeUnixNano int64                  `json:"start_time_unix_nano"`
	EndTimeUnixNano   int64                  `json:"end_time_unix_nano"`
	Status            SpanStatus             `json:"status"`
	Attributes        map[string]interface{} `json:"attributes,omitempty"`
	EmbeddingVector   []float32              `json:"embedding_vector,omitempty"`
	Resource          map[string]string      `json:"resource,omitempty"`
	Links             []SpanLink             `json:"span_links,omitempty"`
}

// Trace groups all spans sharing the same trace_id.
type Trace struct {
	TraceID string `json:"trace_id"`
	Spans   []Span `json:"spans"`
}
