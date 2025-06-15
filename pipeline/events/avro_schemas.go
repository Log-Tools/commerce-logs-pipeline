package events

// Avro schema definitions for log types

const HTTPRequestLogSchema = `{
	"type": "record",
	"name": "HTTPRequestLog",
	"namespace": "com.example.logs",
	"fields": [
		{"name": "ts_ns", "type": "long"},
		{"name": "method", "type": "string"},
		{"name": "path", "type": "string"}, 
		{"name": "status_code", "type": "int"},
		{"name": "response_time_ms", "type": "long"},
		{"name": "bytes_sent", "type": "long"},
		{"name": "client_ip", "type": "string"},
		{"name": "pod_name", "type": "string"}
	]
}`

const ApplicationLogSchema = `{
	"type": "record",
	"name": "ApplicationLog",
	"namespace": "com.example.logs",
	"fields": [
		{"name": "ts_ns", "type": "long"},
		{"name": "level", "type": "string"},
		{"name": "logger", "type": "string"},
		{"name": "thread", "type": "string"},
		{"name": "msg", "type": "string"},
		{"name": "thrown", "type": ["null", "string"], "default": null},
		{"name": "client_ip", "type": ["null", "string"], "default": null},
		{"name": "pod_name", "type": "string"}
	]
}`
