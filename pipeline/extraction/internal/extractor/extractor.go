package extractor

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"

	"pipeline/events"
)

// Extractor extracts structured data from raw logs
type Extractor struct {
	serviceNameExtractor *regexp.Regexp
	requestLineRegex     *regexp.Regexp
}

// Compiles regex patterns for service name extraction and HTTP request parsing
func NewExtractor() *Extractor {
	// Regex to extract service name from pod names
	serviceRegex := regexp.MustCompile(`^([a-z-]+?)(?:-[a-f0-9]+)?-[a-z0-9]+$`)

	// Regex to parse HTTP request line: "METHOD /path HTTP/1.1"
	requestLineRegex := regexp.MustCompile(`^([A-Z]+)\s+([^\s]+)\s+HTTP/[\d.]+$`)

	return &Extractor{
		serviceNameExtractor: serviceRegex,
		requestLineRegex:     requestLineRegex,
	}
}

// ExtractLog extracts structured data based on log type
func (e *Extractor) ExtractLog(rawLine string, source events.LogSource) (interface{}, error) {
	var rawLog events.RawApplicationLogLine
	if err := json.Unmarshal([]byte(rawLine), &rawLog); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	if rawLog.Logs == nil {
		return nil, fmt.Errorf("missing logs substructure")
	}

	// Determine log type based on presence of timeMillis vs instant and contextMap
	isAccessLog := rawLog.Logs.TimeMillis != nil && rawLog.Logs.ContextMap != nil

	if isAccessLog {
		return e.extractHTTPRequestLog(&rawLog, source)
	} else {
		return e.extractApplicationLog(&rawLog, source)
	}
}

// extractHTTPRequestLog extracts structured HTTP request data
func (e *Extractor) extractHTTPRequestLog(rawLog *events.RawApplicationLogLine, source events.LogSource) (*events.HTTPRequestLog, error) {
	// Extract timestamp from timeMillis
	if rawLog.Logs.TimeMillis == nil {
		return nil, fmt.Errorf("missing timeMillis for HTTP request log")
	}
	timestampNanos := *rawLog.Logs.TimeMillis * 1e6

	// Extract pod name
	podName := ""
	if rawLog.Kubernetes != nil {
		podName = rawLog.Kubernetes.PodName
	}

	// Parse contextMap for HTTP fields
	ctx := rawLog.Logs.ContextMap
	if ctx == nil {
		return nil, fmt.Errorf("missing contextMap for HTTP request log")
	}

	// Extract request line (METHOD /path HTTP/1.1)
	requestLine, ok := ctx["requestLine"].(string)
	if !ok {
		return nil, fmt.Errorf("missing or invalid requestLine in contextMap")
	}

	// Parse method and path from request line
	matches := e.requestLineRegex.FindStringSubmatch(requestLine)
	if len(matches) < 3 {
		return nil, fmt.Errorf("failed to parse request line: %s", requestLine)
	}
	method := matches[1]
	path := matches[2]

	// Extract status code
	statusCode := 0
	if sc, ok := ctx["statusCode"]; ok {
		switch v := sc.(type) {
		case float64:
			statusCode = int(v)
		case int:
			statusCode = v
		case string:
			if parsed, err := strconv.Atoi(v); err == nil {
				statusCode = parsed
			}
		}
	}

	// Extract bytes sent
	bytesSent := int64(0)
	if bs, ok := ctx["bytesSent"]; ok {
		switch v := bs.(type) {
		case float64:
			bytesSent = int64(v)
		case int:
			bytesSent = int64(v)
		case int64:
			bytesSent = v
		case string:
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				bytesSent = parsed
			}
		}
	}

	// Extract response time (processMillis)
	responseTimeMs := int64(0)
	if rt, ok := ctx["processMillis"]; ok {
		switch v := rt.(type) {
		case float64:
			responseTimeMs = int64(v)
		case int:
			responseTimeMs = int64(v)
		case int64:
			responseTimeMs = v
		case string:
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				responseTimeMs = parsed
			}
		}
	}

	// Extract client IP (remoteHost)
	clientIP := ""
	if rh, ok := ctx["remoteHost"].(string); ok {
		clientIP = rh
	}

	return &events.HTTPRequestLog{
		TimestampNanos: timestampNanos,
		Method:         method,
		Path:           path,
		StatusCode:     statusCode,
		ResponseTimeMs: responseTimeMs,
		BytesSent:      bytesSent,
		ClientIP:       clientIP,
		PodName:        podName,
	}, nil
}

// extractApplicationLog extracts structured application log data
func (e *Extractor) extractApplicationLog(rawLog *events.RawApplicationLogLine, source events.LogSource) (*events.ApplicationLog, error) {
	// Extract timestamp from instant structure
	if rawLog.Logs.Instant == nil {
		return nil, fmt.Errorf("missing instant timestamp for application log")
	}

	epochSecond := rawLog.Logs.Instant.EpochSecond
	nanoOfSecond := rawLog.Logs.Instant.NanoOfSecond
	timestampNanos := epochSecond*1e9 + nanoOfSecond

	if timestampNanos == 0 {
		return nil, fmt.Errorf("invalid timestamp: epochSecond=%d, nanoOfSecond=%d", epochSecond, nanoOfSecond)
	}

	// Extract pod name
	podName := ""
	if rawLog.Kubernetes != nil {
		podName = rawLog.Kubernetes.PodName
	}

	// Extract exception information if present
	var thrownStr *string
	if rawLog.Logs.Thrown != nil {
		var thrownData interface{}
		if err := json.Unmarshal(*rawLog.Logs.Thrown, &thrownData); err == nil {
			switch v := thrownData.(type) {
			case string:
				thrownStr = &v
			case map[string]interface{}:
				if bytes, err := json.Marshal(v); err == nil {
					str := string(bytes)
					thrownStr = &str
				}
			}
		}
	}

	// Extract client IP from MDC if available
	clientIP := ""
	// Note: Would need to check if client IP is available in contextMap/MDC
	// This is application-specific and may vary

	return &events.ApplicationLog{
		TimestampNanos: timestampNanos,
		Level:          rawLog.Logs.Level,
		Logger:         rawLog.Logs.LoggerName,
		Thread:         rawLog.Logs.Thread,
		Message:        rawLog.Logs.Message,
		Thrown:         thrownStr,
		ClientIP:       clientIP,
		PodName:        podName,
	}, nil
}

// ValidateHTTPRequestLog validates an HTTP request log
func (e *Extractor) ValidateHTTPRequestLog(log *events.HTTPRequestLog) error {
	if log.TimestampNanos <= 0 {
		return fmt.Errorf("invalid timestamp: %d", log.TimestampNanos)
	}
	if log.Method == "" {
		return fmt.Errorf("missing HTTP method")
	}
	if log.Path == "" {
		return fmt.Errorf("missing request path")
	}
	if log.PodName == "" {
		return fmt.Errorf("missing pod name")
	}
	return nil
}

// ValidateApplicationLog validates an application log
func (e *Extractor) ValidateApplicationLog(log *events.ApplicationLog) error {
	if log.TimestampNanos <= 0 {
		return fmt.Errorf("invalid timestamp: %d", log.TimestampNanos)
	}
	if log.Level == "" {
		return fmt.Errorf("missing log level")
	}
	if log.Logger == "" {
		return fmt.Errorf("missing logger name")
	}
	if log.Message == "" {
		return fmt.Errorf("missing log message")
	}
	if log.PodName == "" {
		return fmt.Errorf("missing pod name")
	}
	return nil
}

// ValidateExtractedLog validates either HTTP request or application log
func (e *Extractor) ValidateExtractedLog(log interface{}) error {
	switch l := log.(type) {
	case *events.HTTPRequestLog:
		return e.ValidateHTTPRequestLog(l)
	case *events.ApplicationLog:
		return e.ValidateApplicationLog(l)
	default:
		return fmt.Errorf("unknown log type: %T", log)
	}
}
