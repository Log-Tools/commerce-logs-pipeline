package extractor

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"pipeline/events"
)

// Extractor extracts structured data from raw logs
type Extractor struct {
	serviceNameExtractor *regexp.Regexp
	requestLineRegex     *regexp.Regexp
	logLevelRegex        *regexp.Regexp
}

// Compiles regex patterns for service name extraction and HTTP request parsing
func NewExtractor() *Extractor {
	// Regex to extract service name from pod names
	serviceRegex := regexp.MustCompile(`^([a-z-]+?)(?:-[a-f0-9]+)?-[a-z0-9]+$`)

	// Regex to parse request line: "METHOD /path PROTOCOL/1.1"
	// Allow underscores and other characters in method names to capture non-standard methods
	// Capture protocol to filter out non-HTTP protocols (RTSP, etc.)
	requestLineRegex := regexp.MustCompile(`^([A-Z_]+)\s+([^\s]+)\s+([A-Z]+)/[\d.]+$`)

	// Regex to extract log level from container log messages
	logLevelRegex := regexp.MustCompile(`(?i)\b(TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL)\b`)

	return &Extractor{
		serviceNameExtractor: serviceRegex,
		requestLineRegex:     requestLineRegex,
		logLevelRegex:        logLevelRegex,
	}
}

// ExtractLog extracts structured data based on log type
func (e *Extractor) ExtractLog(rawLine string, source events.LogSource) (interface{}, error) {
	// First, try to parse as structured application log (Format 1)
	var appLog events.RawApplicationLogLine
	if err := json.Unmarshal([]byte(rawLine), &appLog); err == nil && appLog.Logs != nil {
		// Format 1: Structured logs with Logs substructure
		return e.extractFromStructuredLog(&appLog, source, rawLine)
	}

	// If that fails or no Logs field, try container log format (Format 2)
	var containerLog events.RawContainerLogLine
	if err := json.Unmarshal([]byte(rawLine), &containerLog); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Format 2: Container logs without Logs substructure
	result, err := e.extractFromContainerLog(&containerLog, source)
	if err != nil {
		return nil, err
	}
	// Handle the case where extractFromContainerLog returns nil for empty messages
	if result == nil {
		return nil, nil
	}
	return result, nil
}

// extractFromStructuredLog extracts from Format 1 (with Logs substructure)
func (e *Extractor) extractFromStructuredLog(rawLog *events.RawApplicationLogLine, source events.LogSource, rawLine string) (interface{}, error) {
	if rawLog.Logs == nil {
		return nil, fmt.Errorf("missing logs substructure")
	}

	// Determine log type by checking for HTTP-specific fields in contextMap
	// HTTP request logs have requestLine, statusCode, etc.
	// Application logs have sourceClassName, sourceMethodName, or other non-HTTP fields
	isAccessLog := e.isHTTPRequestLog(rawLog.Logs)

	if isAccessLog {
		result, err := e.extractHTTPRequestLog(rawLog, source, rawLine)
		if err != nil {
			return nil, err
		}
		// Handle the case where extractHTTPRequestLog returns nil for malformed requests
		if result == nil {
			return nil, nil
		}
		return result, nil
	} else {
		result, err := e.extractApplicationLog(rawLog, source)
		if err != nil {
			return nil, err
		}
		// Handle the case where extractApplicationLog returns nil for empty messages
		if result == nil {
			return nil, nil
		}
		return result, nil
	}
}

// isHTTPRequestLog determines if a log is an HTTP request log by checking contextMap contents or Apache fields
func (e *Extractor) isHTTPRequestLog(logs *events.RawLogs) bool {
	// Check for Apache access log format first (has requestFirstLine or status fields)
	if logs.RequestFirstLine != "" || logs.Status != "" {
		return true
	}

	// Check for traditional format with timeMillis and contextMap
	if logs.TimeMillis == nil || logs.ContextMap == nil {
		return false
	}

	// Check for HTTP-specific fields in contextMap
	ctx := logs.ContextMap

	// HTTP request logs should have requestLine (required field)
	if _, hasRequestLine := ctx["requestLine"]; hasRequestLine {
		return true
	}

	// Additional check: if it has HTTP-specific fields like statusCode, bytesSent, processMillis
	// but no requestLine, it might still be an HTTP log with missing requestLine
	hasHTTPFields := false
	httpFields := []string{"statusCode", "bytesSent", "processMillis", "remoteHost"}
	for _, field := range httpFields {
		if _, exists := ctx[field]; exists {
			hasHTTPFields = true
			break
		}
	}

	// If it has HTTP fields but no application-specific fields, assume it's HTTP
	if hasHTTPFields {
		appFields := []string{"sourceClassName", "sourceMethodName"}
		for _, field := range appFields {
			if _, exists := ctx[field]; exists {
				// Has application-specific fields, so it's not an HTTP log
				return false
			}
		}
		return true
	}

	return false
}

// extractFromContainerLog extracts from Format 2 (container logs)
func (e *Extractor) extractFromContainerLog(rawLog *events.RawContainerLogLine, source events.LogSource) (*events.ApplicationLog, error) {
	// Skip empty log messages - these are common in container logs and not useful
	// Return nil, nil to indicate this message should be silently skipped
	if strings.TrimSpace(rawLog.Log) == "" {
		return nil, nil
	}

	// Parse timestamp from @timestamp or time field
	timestampNanos, err := e.parseContainerLogTimestamp(rawLog)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	// Extract pod name
	podName := ""
	if rawLog.Kubernetes != nil {
		podName = rawLog.Kubernetes.PodName
	}

	// Extract log level from message content
	level := e.extractLogLevel(rawLog.Log)

	// Extract logger name (use container name or fallback)
	logger := "container"
	if podName != "" {
		// Extract container/service name from pod name
		// Handle both simple names (api-123-456) and compound names (backgroundprocessing-xyz-uvw)
		if matches := e.serviceNameExtractor.FindStringSubmatch(podName); len(matches) > 1 {
			logger = matches[1]
		} else {
			// Fallback: try to extract first part before first dash and digits
			parts := strings.Split(podName, "-")
			if len(parts) > 0 {
				logger = parts[0]
			}
		}
	}

	return &events.ApplicationLog{
		TimestampNanos: timestampNanos,
		Level:          level,
		Logger:         logger,
		Thread:         "", // Container logs don't have thread info
		Message:        rawLog.Log,
		Thrown:         nil, // Container logs don't have structured exceptions
		ClientIP:       "",  // Not available in container logs
		PodName:        podName,
	}, nil
}

// parseContainerLogTimestamp parses ISO 8601 timestamp from container logs
func (e *Extractor) parseContainerLogTimestamp(rawLog *events.RawContainerLogLine) (int64, error) {
	// Try @timestamp first, then time field
	timestampStr := rawLog.Timestamp
	if timestampStr == "" {
		timestampStr = rawLog.Time
	}

	if timestampStr == "" {
		return 0, fmt.Errorf("no timestamp field found")
	}

	// Parse ISO 8601 timestamp
	t, err := time.Parse(time.RFC3339Nano, timestampStr)
	if err != nil {
		return 0, fmt.Errorf("invalid timestamp format: %s", timestampStr)
	}

	return t.UnixNano(), nil
}

// extractLogLevel extracts log level from container log message
func (e *Extractor) extractLogLevel(message string) string {
	matches := e.logLevelRegex.FindStringSubmatch(message)
	if len(matches) > 1 {
		return strings.ToUpper(matches[1])
	}
	return "INFO" // Default fallback
}

// isStandardHTTPMethod checks if the given method is a standard HTTP method
func (e *Extractor) isStandardHTTPMethod(method string) bool {
	standardMethods := map[string]bool{
		"GET":     true,
		"POST":    true,
		"PUT":     true,
		"DELETE":  true,
		"HEAD":    true,
		"OPTIONS": true,
		"PATCH":   true,
		"TRACE":   true,
		"CONNECT": true,
	}
	return standardMethods[method]
}

// extractHTTPRequestLog extracts structured HTTP request data
func (e *Extractor) extractHTTPRequestLog(rawLog *events.RawApplicationLogLine, source events.LogSource, rawLine string) (*events.HTTPRequestLog, error) {
	// Extract timestamp - try timeMillis first, then extract from root level for Apache logs
	var timestampNanos int64
	if rawLog.Logs.TimeMillis != nil {
		timestampNanos = *rawLog.Logs.TimeMillis * 1e6
	} else {
		// Apache access logs don't have timeMillis, extract from root level timestamp
		var containerLog events.RawContainerLogLine
		if err := json.Unmarshal([]byte(rawLine), &containerLog); err == nil {
			// Parse timestamp from root level @timestamp or time field
			timestampNanos, err = e.parseContainerLogTimestamp(&containerLog)
			if err != nil {
				return nil, fmt.Errorf("failed to parse Apache access log timestamp: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to parse Apache access log for timestamp: %w", err)
		}
	}

	// Extract pod name
	podName := ""
	if rawLog.Kubernetes != nil {
		podName = rawLog.Kubernetes.PodName
	}

	// Extract HTTP request data - handle both contextMap format and Apache format
	var method, path, clientIP string
	var statusCode int
	var bytesSent, responseTimeMs int64

	if rawLog.Logs.ContextMap != nil {
		// Traditional format with contextMap
		ctx := rawLog.Logs.ContextMap

		// Extract request line (METHOD /path HTTP/1.1)
		requestLine, ok := ctx["requestLine"].(string)
		if !ok {
			return nil, fmt.Errorf("missing or invalid requestLine in contextMap")
		}

		// Handle malformed/incomplete requests where requestLine is "-"
		if requestLine == "-" {
			// Skip malformed requests - return nil to indicate no event should be generated
			return nil, nil
		}

		// Parse method, path, and protocol from request line
		matches := e.requestLineRegex.FindStringSubmatch(requestLine)
		if len(matches) < 4 {
			return nil, fmt.Errorf("failed to parse request line: %s", requestLine)
		}
		method = matches[1]
		path = matches[2]
		protocol := matches[3]

		// Skip non-HTTP protocols (e.g., RTSP, FTP, etc.)
		if protocol != "HTTP" {
			// Skip non-HTTP protocols - return nil to indicate no event should be generated
			return nil, nil
		}

		// Skip non-standard HTTP methods (e.g., SSTP_DUPLEX_POST, etc.)
		if !e.isStandardHTTPMethod(method) {
			// Skip non-standard methods - return nil to indicate no event should be generated
			return nil, nil
		}

		// Extract status code
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
		if rh, ok := ctx["remoteHost"].(string); ok {
			clientIP = rh
		}
	} else {
		// Apache access log format - fields are directly in logs object
		// Extract request line from requestFirstLine field
		requestLine := rawLog.Logs.RequestFirstLine
		if requestLine == "" {
			return nil, fmt.Errorf("missing requestFirstLine in Apache access log")
		}

		// Handle malformed/incomplete requests where requestFirstLine is "-"
		if requestLine == "-" {
			// Skip malformed requests - return nil to indicate no event should be generated
			return nil, nil
		}

		// Parse method, path, and protocol from request line
		matches := e.requestLineRegex.FindStringSubmatch(requestLine)
		if len(matches) < 4 {
			return nil, fmt.Errorf("failed to parse Apache request line: %s", requestLine)
		}
		method = matches[1]
		path = matches[2]
		protocol := matches[3]

		// Skip non-HTTP protocols (e.g., RTSP, FTP, etc.)
		if protocol != "HTTP" {
			// Skip non-HTTP protocols - return nil to indicate no event should be generated
			return nil, nil
		}

		// Skip non-standard HTTP methods (e.g., SSTP_DUPLEX_POST, etc.)
		if !e.isStandardHTTPMethod(method) {
			// Skip non-standard methods - return nil to indicate no event should be generated
			return nil, nil
		}

		// Extract status code
		if rawLog.Logs.Status != "" {
			if parsed, err := strconv.Atoi(rawLog.Logs.Status); err == nil {
				statusCode = parsed
			}
		}

		// Extract bytes sent
		if rawLog.Logs.Bytes != "" && rawLog.Logs.Bytes != "-" {
			if parsed, err := strconv.ParseInt(rawLog.Logs.Bytes, 10, 64); err == nil {
				bytesSent = parsed
			}
		}

		// Extract response time
		if rawLog.Logs.ResponseTime != "" {
			if parsed, err := strconv.ParseInt(rawLog.Logs.ResponseTime, 10, 64); err == nil {
				responseTimeMs = parsed
			}
		}

		// Extract client IP
		clientIP = rawLog.Logs.RemoteHost
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
	// Extract timestamp - handle both instant and timeMillis formats
	var timestampNanos int64

	if rawLog.Logs.Instant != nil {
		// Use instant structure (epochSecond + nanoOfSecond)
		epochSecond := rawLog.Logs.Instant.EpochSecond
		nanoOfSecond := rawLog.Logs.Instant.NanoOfSecond
		timestampNanos = epochSecond*1e9 + nanoOfSecond

		if timestampNanos == 0 {
			return nil, fmt.Errorf("invalid timestamp: epochSecond=%d, nanoOfSecond=%d", epochSecond, nanoOfSecond)
		}
	} else if rawLog.Logs.TimeMillis != nil {
		// Use timeMillis (convert from milliseconds to nanoseconds)
		timestampNanos = *rawLog.Logs.TimeMillis * 1e6
	} else {
		return nil, fmt.Errorf("missing timestamp (neither instant nor timeMillis found)")
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

	// Skip empty log messages - these are not useful for analysis
	// Return nil, nil to indicate this message should be silently skipped
	if strings.TrimSpace(rawLog.Logs.Message) == "" {
		return nil, nil
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
	if log == nil {
		return fmt.Errorf("cannot validate nil HTTP request log")
	}
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
	if log == nil {
		return fmt.Errorf("cannot validate nil application log")
	}
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

// ValidateExtractedLog validates HTTP request, application, or proxy log
func (e *Extractor) ValidateExtractedLog(log interface{}) error {
	if log == nil {
		return fmt.Errorf("cannot validate nil log")
	}

	switch l := log.(type) {
	case *events.HTTPRequestLog:
		return e.ValidateHTTPRequestLog(l)
	case *events.ApplicationLog:
		return e.ValidateApplicationLog(l)
	case *events.ProxyLog:
		return e.ValidateProxyLog(l)
	default:
		return fmt.Errorf("unknown log type: %T", log)
	}
}

// extractProxyLog extracts structured proxy log data from Apache access log format
func (e *Extractor) extractProxyLog(rawLog *events.RawApplicationLogLine, source events.LogSource, rawLine string) (*events.ProxyLog, error) {
	// Extract timestamp - try timeMillis first, then extract from root level for Apache logs
	var timestampNanos int64
	if rawLog.Logs.TimeMillis != nil {
		timestampNanos = *rawLog.Logs.TimeMillis * 1e6
	} else {
		// Apache access logs don't have timeMillis, extract from root level timestamp
		var containerLog events.RawContainerLogLine
		if err := json.Unmarshal([]byte(rawLine), &containerLog); err == nil {
			// Parse timestamp from root level @timestamp or time field
			timestampNanos, err = e.parseContainerLogTimestamp(&containerLog)
			if err != nil {
				return nil, fmt.Errorf("failed to parse Apache proxy log timestamp: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to parse Apache proxy log for timestamp: %w", err)
		}
	}

	// Extract pod name
	podName := ""
	podIP := ""
	if rawLog.Kubernetes != nil {
		podName = rawLog.Kubernetes.PodName
		podIP = rawLog.Kubernetes.PodIP
	}

	// Extract proxy request data - handle both contextMap format and Apache format
	var method, path, clientIP, localServerName, remoteUser, referer, userAgent, cacheStatus string
	var statusCode int
	var bytesSent, responseTimeMs int64

	if rawLog.Logs.ContextMap != nil {
		// Traditional format with contextMap (unlikely for proxy logs, but handle it)
		ctx := rawLog.Logs.ContextMap

		// Extract request line (METHOD /path HTTP/1.1)
		requestLine, ok := ctx["requestLine"].(string)
		if !ok {
			return nil, fmt.Errorf("missing or invalid requestLine in contextMap")
		}

		// Parse method, path, and protocol from request line
		matches := e.requestLineRegex.FindStringSubmatch(requestLine)
		if len(matches) < 4 {
			return nil, fmt.Errorf("failed to parse request line: %s", requestLine)
		}
		method = matches[1]
		path = matches[2]
		protocol := matches[3]

		// Skip non-HTTP protocols (e.g., RTSP, FTP, etc.)
		if protocol != "HTTP" {
			// Skip non-HTTP protocols - return nil to indicate no event should be generated
			return nil, nil
		}

		// Skip non-standard HTTP methods (e.g., SSTP_DUPLEX_POST, etc.)
		if !e.isStandardHTTPMethod(method) {
			// Skip non-standard methods - return nil to indicate no event should be generated
			return nil, nil
		}

		// Extract other fields from contextMap
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

		if rt, ok := ctx["responseTime"]; ok {
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

		if rh, ok := ctx["remoteHost"].(string); ok {
			clientIP = rh
		}
	} else {
		// Apache proxy log format - fields are directly in logs object
		// Extract request line from requestFirstLine field
		requestLine := rawLog.Logs.RequestFirstLine
		if requestLine == "" {
			return nil, fmt.Errorf("missing requestFirstLine in Apache proxy log")
		}

		// Handle malformed/incomplete requests where requestFirstLine is "-"
		if requestLine == "-" {
			// Skip malformed requests - return nil to indicate no event should be generated
			return nil, nil
		} else {
			// Parse method, path, and protocol from request line
			matches := e.requestLineRegex.FindStringSubmatch(requestLine)
			if len(matches) < 4 {
				return nil, fmt.Errorf("failed to parse Apache proxy request line: %s", requestLine)
			}
			method = matches[1]
			path = matches[2]
			protocol := matches[3]

			// Skip non-HTTP protocols (e.g., RTSP, FTP, etc.)
			if protocol != "HTTP" {
				// Skip non-HTTP protocols - return nil to indicate no event should be generated
				return nil, nil
			}

			// Skip non-standard HTTP methods (e.g., SSTP_DUPLEX_POST, etc.)
			if !e.isStandardHTTPMethod(method) {
				// Skip non-standard methods - return nil to indicate no event should be generated
				return nil, nil
			}
		}

		// Extract status code
		if rawLog.Logs.Status != "" {
			if parsed, err := strconv.Atoi(rawLog.Logs.Status); err == nil {
				statusCode = parsed
			}
		}

		// Extract bytes sent
		if rawLog.Logs.Bytes != "" && rawLog.Logs.Bytes != "-" {
			if parsed, err := strconv.ParseInt(rawLog.Logs.Bytes, 10, 64); err == nil {
				bytesSent = parsed
			}
		}

		// Extract response time
		if rawLog.Logs.ResponseTime != "" {
			if parsed, err := strconv.ParseInt(rawLog.Logs.ResponseTime, 10, 64); err == nil {
				responseTimeMs = parsed
			}
		}

		// Extract proxy-specific fields
		clientIP = rawLog.Logs.RemoteHost
		localServerName = rawLog.Logs.LocalServerName
		remoteUser = rawLog.Logs.RemoteUser
		referer = rawLog.Logs.Referer
		userAgent = rawLog.Logs.UserAgent
		cacheStatus = rawLog.Logs.CacheStatus
	}

	return &events.ProxyLog{
		TimestampNanos:  timestampNanos,
		Method:          method,
		Path:            path,
		StatusCode:      statusCode,
		ResponseTimeMs:  responseTimeMs,
		BytesSent:       bytesSent,
		ClientIP:        clientIP,
		LocalServerName: localServerName,
		RemoteUser:      remoteUser,
		Referer:         referer,
		UserAgent:       userAgent,
		CacheStatus:     cacheStatus,
		PodName:         podName,
		PodIP:           podIP,
	}, nil
}

// ValidateProxyLog validates a proxy log
func (e *Extractor) ValidateProxyLog(log *events.ProxyLog) error {
	if log == nil {
		return fmt.Errorf("cannot validate nil proxy log")
	}
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

// ExtractProxyLog extracts structured proxy log data specifically from Raw.ProxyLogs topic
func (e *Extractor) ExtractProxyLog(rawLine string, source events.LogSource) (interface{}, error) {
	// First, try to parse as structured application log (Format 1)
	var appLog events.RawApplicationLogLine
	if err := json.Unmarshal([]byte(rawLine), &appLog); err == nil && appLog.Logs != nil {
		// Check if this is actually a proxy access log or a regular container log
		if e.isProxyLog(appLog.Logs) {
			// Format 1: Structured Apache access logs with proxy-specific fields
			result, err := e.extractProxyLog(&appLog, source, rawLine)
			if err != nil {
				return nil, err
			}
			// Handle the case where extractProxyLog returns nil for malformed requests
			if result == nil {
				return nil, nil
			}
			return result, nil
		} else {
			// Format 1: Regular application log from proxy container (startup, errors, etc.)
			return e.extractApplicationLog(&appLog, source)
		}
	}

	// If that fails, try container log format (Format 2) - proxy container logs
	var containerLog events.RawContainerLogLine
	if err := json.Unmarshal([]byte(rawLine), &containerLog); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Format 2: Container logs from proxy service (startup, error logs, etc.)
	result, err := e.extractFromContainerLog(&containerLog, source)
	if err != nil {
		return nil, err
	}
	// Handle the case where extractFromContainerLog returns nil for empty messages
	if result == nil {
		return nil, nil
	}
	return result, nil
}

// isProxyLog determines if a log should be processed as a proxy log
// This is primarily determined by the topic it comes from (Raw.ProxyLogs)
func (e *Extractor) isProxyLog(logs *events.RawLogs) bool {
	// Check for Apache proxy log format (has requestFirstLine and proxy-specific fields)
	if logs.RequestFirstLine != "" && logs.LocalServerName != "" {
		return true
	}

	// Check for proxy-specific fields that distinguish it from regular HTTP request logs
	if logs.LocalServerName != "" || logs.CacheStatus != "" {
		return true
	}

	return false
}
