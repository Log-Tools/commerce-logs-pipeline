package events

import (
	"encoding/json"
)

// RawApplicationLogLine represents the raw JSON structure from the logging system (Format 1)
type RawApplicationLogLine struct {
	Logs       *RawLogs       `json:"Logs"`
	Kubernetes *RawKubernetes `json:"kubernetes"`
}

// RawContainerLogLine represents container logs without nested logs structure (Format 2)
type RawContainerLogLine struct {
	// Core fields
	Timestamp  string         `json:"@timestamp"`
	Time       string         `json:"time,omitempty"`
	Stream     string         `json:"stream,omitempty"`
	Log        string         `json:"log"`
	Process    string         `json:"_p,omitempty"`
	RecordDate string         `json:"record_date,omitempty"`
	Kubernetes *RawKubernetes `json:"kubernetes"`
}

// RawLogs represents the nested logs structure in the raw JSON
type RawLogs struct {
	// For HTTP request logs (access logs)
	TimeMillis *int64                 `json:"timeMillis,omitempty"`
	ContextMap map[string]interface{} `json:"contextMap,omitempty"`

	// For application logs
	Instant    *RawInstant      `json:"instant,omitempty"`
	Level      string           `json:"level,omitempty"`
	LoggerName string           `json:"loggerName,omitempty"`
	Thread     string           `json:"thread,omitempty"`
	Message    string           `json:"message,omitempty"`
	Thrown     *json.RawMessage `json:"thrown,omitempty"`
}

// RawInstant represents the timestamp structure for application logs
type RawInstant struct {
	EpochSecond  int64 `json:"epochSecond"`
	NanoOfSecond int64 `json:"nanoOfSecond"`
}

// RawKubernetes represents the Kubernetes metadata in the raw JSON
type RawKubernetes struct {
	PodName string `json:"pod_name"`
}

// HTTPRequestLog represents structured HTTP request/access log data
type HTTPRequestLog struct {
	// Timestamp in nanoseconds
	TimestampNanos int64 `json:"ts_ns" avro:"ts_ns"`

	// HTTP method (GET, POST, PUT, etc.)
	Method string `json:"method" avro:"method"`

	// Request path/URI
	Path string `json:"path" avro:"path"`

	// HTTP status code
	StatusCode int `json:"status_code" avro:"status_code"`

	// Response time in milliseconds
	ResponseTimeMs int64 `json:"response_time_ms" avro:"response_time_ms"`

	// Response size in bytes
	BytesSent int64 `json:"bytes_sent" avro:"bytes_sent"`

	// Client IP address
	ClientIP string `json:"client_ip" avro:"client_ip"`

	// Kubernetes pod name
	PodName string `json:"pod_name" avro:"pod_name"`
}

// ApplicationLog represents structured application log data
type ApplicationLog struct {
	// Timestamp in nanoseconds
	TimestampNanos int64 `json:"ts_ns" avro:"ts_ns"`

	// Log level (INFO, DEBUG, ERROR, etc.)
	Level string `json:"level" avro:"level"`

	// Logger name (Java class name)
	Logger string `json:"logger" avro:"logger"`

	// Thread name where the log originated
	Thread string `json:"thread" avro:"thread"`

	// Main log message
	Message string `json:"msg" avro:"msg"`

	// Serialized exception stack trace (optional)
	Thrown *string `json:"thrown,omitempty" avro:"thrown"`

	// Client IP (if available from MDC)
	ClientIP string `json:"client_ip,omitempty" avro:"client_ip"`

	// Kubernetes pod name
	PodName string `json:"pod_name" avro:"pod_name"`
}

// LogSource contains metadata about where the log originated
type LogSource struct {
	// Service name (api, backoffice, backgroundprocessing)
	Service string `json:"service"`

	// Environment (P1, D1, S1, etc.)
	Environment string `json:"environment"`

	// Subscription ID
	Subscription string `json:"subscription"`

	// Kubernetes pod name
	PodName string `json:"pod_name,omitempty"`
}

// ExtractionError represents an error that occurred during log extraction
type ExtractionError struct {
	// Original raw log line that failed to parse
	RawLine string `json:"raw_line"`

	// Error message describing what went wrong
	Error string `json:"error"`

	// Type of error (parse_error, validation_error, etc.)
	ErrorType string `json:"error_type"`

	// Timestamp when the error occurred
	Timestamp int64 `json:"timestamp"`

	// Source information
	Source LogSource `json:"source"`
}

// ExtractedLog represents either an HTTPRequestLog or ApplicationLog
// This is a union type that can hold either structured log type
type ExtractedLog struct {
	HTTPRequest    *HTTPRequestLog `json:"http_request,omitempty"`
	ApplicationLog *ApplicationLog `json:"application_log,omitempty"`
}
