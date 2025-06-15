package extractor

import (
	"fmt"
	"testing"

	"pipeline/events"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractor_ExtractLog_HTTPRequest(t *testing.T) {
	extractor := NewExtractor()

	accessLogJSON := `{
		"Logs": {
			"timeMillis": 1734243648463,
			"contextMap": {
				"requestLine": "GET /api/test HTTP/1.1",
				"statusCode": 200,
				"bytesSent": 4538,
				"processMillis": 71,
				"remoteHost": "10.20.30.152"
			}
		},
		"kubernetes": {
			"pod_name": "api-5f7d8c9b4d-x7k2p"
		}
	}`

	source := events.LogSource{Service: "api", Environment: "P1", Subscription: "test"}

	result, err := extractor.ExtractLog(accessLogJSON, source)
	require.NoError(t, err)

	httpLog, ok := result.(*events.HTTPRequestLog)
	require.True(t, ok, "Expected HTTPRequestLog")

	assert.Equal(t, "GET", httpLog.Method)
	assert.Equal(t, "/api/test", httpLog.Path)
	assert.Equal(t, 200, httpLog.StatusCode)
	assert.Equal(t, int64(71), httpLog.ResponseTimeMs)
	assert.Equal(t, int64(4538), httpLog.BytesSent)
	assert.Equal(t, "10.20.30.152", httpLog.ClientIP)
	assert.Equal(t, "api-5f7d8c9b4d-x7k2p", httpLog.PodName)

	// Test validation
	err = extractor.ValidateExtractedLog(result)
	assert.NoError(t, err)
}

func TestExtractor_ExtractLog_Application(t *testing.T) {
	extractor := NewExtractor()

	appLogJSON := `{
		"Logs": {
			"instant": {
				"epochSecond": 1734243648,
				"nanoOfSecond": 463000000
			},
			"level": "INFO",
			"loggerName": "com.example.Service",
			"thread": "main",
			"message": "Test message"
		},
		"kubernetes": {
			"pod_name": "api-5f7d8c9b4d-x7k2p"
		}
	}`

	source := events.LogSource{Service: "api", Environment: "P1", Subscription: "test"}

	result, err := extractor.ExtractLog(appLogJSON, source)
	require.NoError(t, err)

	appLog, ok := result.(*events.ApplicationLog)
	require.True(t, ok, "Expected ApplicationLog")

	assert.Equal(t, "INFO", appLog.Level)
	assert.Equal(t, "com.example.Service", appLog.Logger)
	assert.Equal(t, "main", appLog.Thread)
	assert.Equal(t, "Test message", appLog.Message)
	assert.Equal(t, "api-5f7d8c9b4d-x7k2p", appLog.PodName)

	// Test validation
	err = extractor.ValidateExtractedLog(result)
	assert.NoError(t, err)
}

// Test real commerce API access log (from user's example)
func TestExtractor_RealCommerceAPIAccessLog(t *testing.T) {
	extractor := NewExtractor()

	// Real example from user's access logs
	realAccessLog := `{
		"Logs": {
			"timeMillis": 1734243648463,
			"contextMap": {
				"requestLine": "GET /api/commerce/subscriptions/a70f0ca2-8b83-4eb8-8a38-7653d44b05e4/customers/search HTTP/1.1",
				"statusCode": 200,
				"bytesSent": 4538,
				"processMillis": 71,
				"remoteHost": "10.20.30.152"
			}
		},
		"kubernetes": {
			"pod_name": "api-5f7d8c9b4d-x7k2p"
		}
	}`

	source := events.LogSource{Service: "api", Environment: "P1", Subscription: "cp2"}

	result, err := extractor.ExtractLog(realAccessLog, source)
	require.NoError(t, err)

	httpLog, ok := result.(*events.HTTPRequestLog)
	require.True(t, ok, "Expected HTTPRequestLog")

	assert.Equal(t, "GET", httpLog.Method)
	assert.Equal(t, "/api/commerce/subscriptions/a70f0ca2-8b83-4eb8-8a38-7653d44b05e4/customers/search", httpLog.Path)
	assert.Equal(t, 200, httpLog.StatusCode)
	assert.Equal(t, int64(71), httpLog.ResponseTimeMs)
	assert.Equal(t, int64(4538), httpLog.BytesSent)
	assert.Equal(t, "10.20.30.152", httpLog.ClientIP)
	assert.Equal(t, "api-5f7d8c9b4d-x7k2p", httpLog.PodName)
	assert.Equal(t, int64(1734243648463000000), httpLog.TimestampNanos) // Converted to nanoseconds
}

// Test different services: backoffice
func TestExtractor_BackofficeService(t *testing.T) {
	extractor := NewExtractor()

	backofficeLog := `{
		"Logs": {
			"instant": {
				"epochSecond": 1734243648,
				"nanoOfSecond": 123456789
			},
			"level": "INFO",
			"loggerName": "com.backoffice.UserService",
			"thread": "scheduled-task-1",
			"message": "Processing user data cleanup"
		},
		"kubernetes": {
			"pod_name": "backoffice-abc123-def456"
		}
	}`

	source := events.LogSource{Service: "backoffice", Environment: "P1", Subscription: "cp2"}

	result, err := extractor.ExtractLog(backofficeLog, source)
	require.NoError(t, err)

	appLog, ok := result.(*events.ApplicationLog)
	require.True(t, ok, "Expected ApplicationLog")

	assert.Equal(t, "INFO", appLog.Level)
	assert.Equal(t, "com.backoffice.UserService", appLog.Logger)
	assert.Equal(t, "scheduled-task-1", appLog.Thread)
	assert.Equal(t, "Processing user data cleanup", appLog.Message)
	assert.Equal(t, "backoffice-abc123-def456", appLog.PodName)
}

// Test different services: backgroundprocessing
func TestExtractor_BackgroundProcessingService(t *testing.T) {
	extractor := NewExtractor()

	backgroundLog := `{
		"Logs": {
			"instant": {
				"epochSecond": 1734243648,
				"nanoOfSecond": 987654321
			},
			"level": "DEBUG",
			"loggerName": "com.backgroundprocessing.EventHandler",
			"thread": "worker-pool-3",
			"message": "Event processed successfully"
		},
		"kubernetes": {
			"pod_name": "backgroundprocessing-xyz789-uvw123"
		}
	}`

	source := events.LogSource{Service: "backgroundprocessing", Environment: "P1", Subscription: "cp2"}

	result, err := extractor.ExtractLog(backgroundLog, source)
	require.NoError(t, err)

	appLog, ok := result.(*events.ApplicationLog)
	require.True(t, ok, "Expected ApplicationLog")

	assert.Equal(t, "DEBUG", appLog.Level)
	assert.Equal(t, "com.backgroundprocessing.EventHandler", appLog.Logger)
	assert.Equal(t, "worker-pool-3", appLog.Thread)
	assert.Equal(t, "Event processed successfully", appLog.Message)
	assert.Equal(t, "backgroundprocessing-xyz789-uvw123", appLog.PodName)
}

// Test application log with exception (thrown field)
func TestExtractor_ApplicationLogWithException(t *testing.T) {
	extractor := NewExtractor()

	logWithException := `{
		"Logs": {
			"instant": {
				"epochSecond": 1734243648,
				"nanoOfSecond": 123456789
			},
			"level": "ERROR",
			"loggerName": "com.api.PaymentService",
			"thread": "http-nio-8080-exec-5",
			"message": "Payment processing failed",
			"thrown": "java.lang.RuntimeException: Payment gateway timeout"
		},
		"kubernetes": {
			"pod_name": "api-payment-abc123-def456"
		}
	}`

	source := events.LogSource{Service: "api", Environment: "P1", Subscription: "cp2"}

	result, err := extractor.ExtractLog(logWithException, source)
	require.NoError(t, err)

	appLog, ok := result.(*events.ApplicationLog)
	require.True(t, ok, "Expected ApplicationLog")

	assert.Equal(t, "ERROR", appLog.Level)
	assert.Equal(t, "Payment processing failed", appLog.Message)
	require.NotNil(t, appLog.Thrown)
	assert.Equal(t, "java.lang.RuntimeException: Payment gateway timeout", *appLog.Thrown)
}

// Test HTTP POST request with different data
func TestExtractor_HTTPPostRequest(t *testing.T) {
	extractor := NewExtractor()

	postRequestLog := `{
		"Logs": {
			"timeMillis": 1734243648463,
			"contextMap": {
				"requestLine": "POST /api/orders HTTP/1.1",
				"statusCode": 201,
				"bytesSent": 1024,
				"processMillis": 145,
				"remoteHost": "192.168.1.100"
			}
		},
		"kubernetes": {
			"pod_name": "api-orders-service-123"
		}
	}`

	source := events.LogSource{Service: "api", Environment: "P1", Subscription: "cp2"}

	result, err := extractor.ExtractLog(postRequestLog, source)
	require.NoError(t, err)

	httpLog, ok := result.(*events.HTTPRequestLog)
	require.True(t, ok, "Expected HTTPRequestLog")

	assert.Equal(t, "POST", httpLog.Method)
	assert.Equal(t, "/api/orders", httpLog.Path)
	assert.Equal(t, 201, httpLog.StatusCode)
	assert.Equal(t, int64(145), httpLog.ResponseTimeMs)
	assert.Equal(t, int64(1024), httpLog.BytesSent)
	assert.Equal(t, "192.168.1.100", httpLog.ClientIP)
}

// Test error cases
func TestExtractor_ErrorCases(t *testing.T) {
	extractor := NewExtractor()
	source := events.LogSource{Service: "test", Environment: "P1", Subscription: "cp2"}

	testCases := []struct {
		name        string
		logJSON     string
		expectedErr string
	}{
		{
			name:        "Invalid JSON",
			logJSON:     `{invalid json}`,
			expectedErr: "failed to parse JSON",
		},
		{
			name:        "Missing Logs structure",
			logJSON:     `{"kubernetes": {"pod_name": "test"}}`,
			expectedErr: "missing logs substructure",
		},
		{
			name:        "Contextmap without timeMillis (routes to app log path)",
			logJSON:     `{"Logs": {"timeMillis": null, "contextMap": {"requestLine": "GET /test HTTP/1.1"}}}`,
			expectedErr: "missing instant timestamp for application log",
		},
		{
			name:        "Missing instant for application log",
			logJSON:     `{"Logs": {"level": "INFO", "message": "test"}}`,
			expectedErr: "missing instant timestamp for application log",
		},
		{
			name:        "Invalid request line format",
			logJSON:     `{"Logs": {"timeMillis": 123456789, "contextMap": {"requestLine": "invalid"}}}`,
			expectedErr: "failed to parse request line",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := extractor.ExtractLog(tc.logJSON, source)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedErr)
		})
	}
}

// Test validation edge cases
func TestExtractor_ValidationEdgeCases(t *testing.T) {
	extractor := NewExtractor()

	testCases := []struct {
		name        string
		log         interface{}
		expectedErr string
	}{
		{
			name: "HTTP log missing method",
			log: &events.HTTPRequestLog{
				TimestampNanos: 1234567890,
				Path:           "/test",
				PodName:        "test-pod",
			},
			expectedErr: "missing HTTP method",
		},
		{
			name: "HTTP log missing path",
			log: &events.HTTPRequestLog{
				TimestampNanos: 1234567890,
				Method:         "GET",
				PodName:        "test-pod",
			},
			expectedErr: "missing request path",
		},
		{
			name: "Application log missing level",
			log: &events.ApplicationLog{
				TimestampNanos: 1234567890,
				Logger:         "test.Logger",
				Message:        "test message",
				PodName:        "test-pod",
			},
			expectedErr: "missing log level",
		},
		{
			name: "Application log missing message",
			log: &events.ApplicationLog{
				TimestampNanos: 1234567890,
				Level:          "INFO",
				Logger:         "test.Logger",
				PodName:        "test-pod",
			},
			expectedErr: "missing log message",
		},
		{
			name:        "Unknown log type",
			log:         "invalid log type",
			expectedErr: "unknown log type",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := extractor.ValidateExtractedLog(tc.log)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedErr)
		})
	}
}

// Test different environments and subscriptions
func TestExtractor_DifferentEnvironments(t *testing.T) {
	extractor := NewExtractor()

	environments := []string{"P1", "D1", "S1"}
	subscriptions := []string{"cp1", "cp2", "cp3"}

	for _, env := range environments {
		for _, sub := range subscriptions {
			t.Run(fmt.Sprintf("env_%s_sub_%s", env, sub), func(t *testing.T) {
				logJSON := `{
					"Logs": {
						"instant": {
							"epochSecond": 1734243648,
							"nanoOfSecond": 123456789
						},
						"level": "INFO",
						"loggerName": "com.test.Service",
						"thread": "main",
						"message": "Test log"
					},
					"kubernetes": {
						"pod_name": "test-service-123"
					}
				}`

				source := events.LogSource{
					Service:      "test-service",
					Environment:  env,
					Subscription: sub,
				}

				result, err := extractor.ExtractLog(logJSON, source)
				require.NoError(t, err)

				appLog, ok := result.(*events.ApplicationLog)
				require.True(t, ok, "Expected ApplicationLog")
				assert.Equal(t, "INFO", appLog.Level)
				assert.Equal(t, "Test log", appLog.Message)
			})
		}
	}
}
