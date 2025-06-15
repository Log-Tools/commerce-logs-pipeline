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
		expectSkip  bool
	}{
		{
			name:        "Invalid JSON",
			logJSON:     `{invalid json}`,
			expectedErr: "failed to parse JSON",
		},
		{
			name:       "Missing both Logs structure and log field (empty message)",
			logJSON:    `{"kubernetes": {"pod_name": "test"}}`,
			expectSkip: true, // This should be skipped silently, not produce an error
		},
		{
			name:        "Contextmap without timeMillis (routes to app log path)",
			logJSON:     `{"Logs": {"timeMillis": null, "contextMap": {"requestLine": "GET /test HTTP/1.1"}}}`,
			expectedErr: "missing timestamp (neither instant nor timeMillis found)",
		},
		{
			name:        "Missing instant for application log",
			logJSON:     `{"Logs": {"level": "INFO", "message": "test"}}`,
			expectedErr: "missing timestamp (neither instant nor timeMillis found)",
		},
		{
			name:        "Invalid request line format",
			logJSON:     `{"Logs": {"timeMillis": 123456789, "contextMap": {"requestLine": "invalid"}}}`,
			expectedErr: "failed to parse request line",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := extractor.ExtractLog(tc.logJSON, source)

			if tc.expectSkip {
				// Should be skipped silently (no error, no result)
				assert.NoError(t, err)
				assert.Nil(t, result)
			} else {
				// Should produce an error
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErr)
			}
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

// Test extraction from container logs (Format 2)
func TestExtractor_ExtractLog_ContainerLogs(t *testing.T) {
	extractor := NewExtractor()

	testCases := []struct {
		name     string
		logJSON  string
		expected events.ApplicationLog
	}{
		{
			name: "Container log with INFO level",
			logJSON: `{
				"@timestamp": "2025-06-13T12:11:57.259321Z",
				"stream": "stderr",
				"_p": "F",
				"log": "INFO: property name: \"ccv2.additional.catalina.opts\"",
				"record_date": "20250613",
				"time": "2025-06-13T12:11:57.259321544Z",
				"kubernetes": {
					"pod_name": "api-869d548fdb-8hzh7"
				}
			}`,
			expected: events.ApplicationLog{
				TimestampNanos: 0, // will verify > 0 in test
				Level:          "INFO",
				Logger:         "api", // extracted from pod name
				Thread:         "",
				Message:        "INFO: property name: \"ccv2.additional.catalina.opts\"",
				Thrown:         nil,
				ClientIP:       "",
				PodName:        "api-869d548fdb-8hzh7",
			},
		},
		{
			name: "Container log with ERROR level",
			logJSON: `{
				"@timestamp": "2025-06-13T15:30:45.123456Z",
				"stream": "stdout",
				"log": "ERROR: Failed to connect to database",
				"kubernetes": {
					"pod_name": "backoffice-abc123-def456"
				}
			}`,
			expected: events.ApplicationLog{
				Level:   "ERROR",
				Logger:  "backoffice",
				Message: "ERROR: Failed to connect to database",
				PodName: "backoffice-abc123-def456",
			},
		},
		{
			name: "Container log with DEBUG level",
			logJSON: `{
				"time": "2025-06-13T08:15:30.987654321Z",
				"log": "DEBUG: Processing request batch",
				"kubernetes": {
					"pod_name": "backgroundprocessing-xyz789-uvw123"
				}
			}`,
			expected: events.ApplicationLog{
				Level:   "DEBUG",
				Logger:  "backgroundprocessing",
				Message: "DEBUG: Processing request batch",
				PodName: "backgroundprocessing-xyz789-uvw123",
			},
		},
		{
			name: "Container log with no explicit level (defaults to INFO)",
			logJSON: `{
				"@timestamp": "2025-06-13T20:45:12.456789Z",
				"log": "Starting application server on port 8080",
				"kubernetes": {
					"pod_name": "api-5f7d8c9b4d-x7k2p"
				}
			}`,
			expected: events.ApplicationLog{
				Level:   "INFO", // default fallback
				Logger:  "api",
				Message: "Starting application server on port 8080",
				PodName: "api-5f7d8c9b4d-x7k2p",
			},
		},
		{
			name: "Container log with WARN level",
			logJSON: `{
				"@timestamp": "2025-06-13T16:22:38.111222Z",
				"log": "WARN: Connection timeout exceeded, retrying...",
				"kubernetes": {
					"pod_name": "proxy-nginx-456def-789ghi"
				}
			}`,
			expected: events.ApplicationLog{
				Level:   "WARN",
				Logger:  "proxy-nginx",
				Message: "WARN: Connection timeout exceeded, retrying...",
				PodName: "proxy-nginx-456def-789ghi",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			source := events.LogSource{Service: "test", Environment: "D1", Subscription: "cp2"}

			result, err := extractor.ExtractLog(tc.logJSON, source)
			require.NoError(t, err)

			appLog, ok := result.(*events.ApplicationLog)
			require.True(t, ok, "Expected ApplicationLog")

			assert.Equal(t, tc.expected.Level, appLog.Level)
			assert.Equal(t, tc.expected.Logger, appLog.Logger)
			assert.Equal(t, tc.expected.Thread, appLog.Thread)
			assert.Equal(t, tc.expected.Message, appLog.Message)
			assert.Equal(t, tc.expected.PodName, appLog.PodName)
			assert.Nil(t, appLog.Thrown)
			assert.Equal(t, "", appLog.ClientIP)

			// Verify timestamp was parsed correctly (if not zero in expected)
			if tc.expected.TimestampNanos != 0 {
				assert.Equal(t, tc.expected.TimestampNanos, appLog.TimestampNanos)
			} else {
				assert.Greater(t, appLog.TimestampNanos, int64(0))
			}

			// Test validation
			err = extractor.ValidateExtractedLog(result)
			assert.NoError(t, err)
		})
	}
}

// Test case for application log with timeMillis and contextMap (should not be misidentified as HTTP log)
func TestExtractor_ApplicationLogWithTimeMillisAndContextMap(t *testing.T) {
	extractor := NewExtractor()
	source := events.LogSource{Service: "backoffice", Environment: "D1", Subscription: "cp2"}

	// This is the actual problematic log from the error report
	logJSON := `{
		"@timestamp": "2025-06-12T14:56:11.743010Z",
		"time": "2025-06-12T14:56:11.743010328Z",
		"kubernetes": {
			"pod_name": "backoffice-5ff5bf97b9-z6rg5"
		},
		"logs": {
			"loggerName": "de.hybris.ccv2.listeners.CCv2LifecycleListener",
			"message": "SHUTDOWN HOOK IN PROCESS",
			"timeMillis": 1749740171742,
			"contextMap": {
				"sourceMethodName": "lambda$new$0",
				"sourceClassName": "de.hybris.ccv2.listeners.CCv2LifecycleListener"
			},
			"threadId": 373,
			"threadPriority": 1,
			"origin": "catalina",
			"thread": "Thread-46",
			"level": "INFO"
		}
	}`

	result, err := extractor.ExtractLog(logJSON, source)
	require.NoError(t, err, "Should successfully extract application log")

	// Should be extracted as ApplicationLog, not HTTPRequestLog
	appLog, ok := result.(*events.ApplicationLog)
	require.True(t, ok, "Expected ApplicationLog, got %T", result)

	// Verify extracted fields
	assert.Equal(t, "INFO", appLog.Level)
	assert.Equal(t, "de.hybris.ccv2.listeners.CCv2LifecycleListener", appLog.Logger)
	assert.Equal(t, "SHUTDOWN HOOK IN PROCESS", appLog.Message)
	assert.Equal(t, "Thread-46", appLog.Thread)
	assert.Equal(t, "backoffice-5ff5bf97b9-z6rg5", appLog.PodName)
	assert.Equal(t, int64(1749740171742*1e6), appLog.TimestampNanos) // timeMillis converted to nanos
	assert.Nil(t, appLog.Thrown)
}

// Test the exact failing case from user's error
func TestExtractor_ExtractLog_UserErrorCase(t *testing.T) {
	extractor := NewExtractor()

	// This is the exact log that was failing
	errorLogJSON := `{
		"@timestamp": "2025-06-13T12:11:57.259321Z",
		"stream": "stderr",
		"_p": "F",
		"log": "INFO: property name: \"ccv2.additional.catalina.opts\"",
		"record_date": "20250613",
		"time": "2025-06-13T12:11:57.259321544Z",
		"kubernetes": {
			"annotations": {
				"ccv2_cx_sap_com_build-code": "20250613.2",
				"cni_projectcalico_org_containerID": "32a88d1d8627d98fb43555e7be24d112a5b7f7577b6792b39285caeb7f7ec58f",
				"ccv2_cx_sap_com_restart-requested-at": "2024-06-30 17:30:34.757",
				"ccv2_cx_sap_com_deployment-id": "1409894",
				"data-ingest_dynatrace_com_injected": "true",
				"oneagent_dynatrace_com_injected": "true",
				"cni_projectcalico_org_podIP": "10.244.1.20/32",
				"fluentbit_io_parser": "mt-api",
				"cni_projectcalico_org_podIPs": "10.244.1.20/32",
				"platform-security-configmap-hash": "2332086959",
				"dynakube_dynatrace_com_injected": "true"
			},
			"container_hash": "c4oemlbtqrccbmanage4z0p.azurecr.io/ccbmanage4/platform@sha256:7dcbb35f5654b0e815bdc740eaae23bfc0b9d17cd742cd43a70daad94a86ec55",
			"container_image": "c4oemlbtqrccbmanage4z0p.azurecr.io/ccbmanage4/platform:hash-363c9483fb455492586247588d8cda2d72ecd6b5276fb212dfa1da521435a4ee",
			"pod_ip": "10.244.1.20",
			"host": "aks-guhn66afpt-25077532-vmss00001b",
			"namespace_name": "default",
			"labels": {
				"app_kubernetes_io_name": "hybris",
				"pod-template-hash": "869d548fdb",
				"ccv2_cx_sap_com_service-version": "default",
				"app_kubernetes_io_part-of": "hybris",
				"ccv2_cx_sap_com_service-name": "api",
				"ccv2_cx_sap_com_platform-aspect": "api",
				"app_kubernetes_io_component": "backend",
				"app_kubernetes_io_managed-by": "hybris-operator"
			},
			"pod_name": "api-869d548fdb-8hzh7",
			"container_name": "platform",
			"pod_id": "eace775d-76bc-447b-866d-bb3f22f1337c",
			"docker_id": "05ce9b66487d08c56bba9a7f3f67d812ae63b4add937909191f10fdc72177c8a"
		}
	}`

	source := events.LogSource{Service: "", Environment: "", Subscription: ""}

	result, err := extractor.ExtractLog(errorLogJSON, source)
	require.NoError(t, err, "This should no longer fail with 'missing logs substructure'")

	appLog, ok := result.(*events.ApplicationLog)
	require.True(t, ok, "Expected ApplicationLog")

	assert.Equal(t, "INFO", appLog.Level)
	assert.Equal(t, "api", appLog.Logger) // extracted from pod name
	assert.Equal(t, "", appLog.Thread)    // container logs don't have thread info
	assert.Equal(t, "INFO: property name: \"ccv2.additional.catalina.opts\"", appLog.Message)
	assert.Equal(t, "api-869d548fdb-8hzh7", appLog.PodName)
	assert.Nil(t, appLog.Thrown)
	assert.Equal(t, "", appLog.ClientIP)
	assert.Greater(t, appLog.TimestampNanos, int64(0))

	// Test validation passes
	err = extractor.ValidateExtractedLog(result)
	assert.NoError(t, err)
}

// Test handling of empty log messages
func TestExtractor_EmptyLogMessages(t *testing.T) {
	extractor := NewExtractor()
	source := events.LogSource{Service: "test", Environment: "D1", Subscription: "cp2"}

	testCases := []struct {
		name          string
		logJSON       string
		expectError   bool
		expectSkipped bool
		errorMsg      string
	}{
		{
			name: "Container log with empty message should be skipped",
			logJSON: `{
				"@timestamp": "2025-06-12T15:30:33.192086Z",
				"stream": "stdout",
				"log": "",
				"kubernetes": {
					"pod_name": "backgroundprocessing-58c69494d9-7xksj"
				}
			}`,
			expectError:   false,
			expectSkipped: true,
		},
		{
			name: "Container log with whitespace-only message should be skipped",
			logJSON: `{
				"@timestamp": "2025-06-12T15:30:33.192086Z",
				"stream": "stdout",
				"log": "   \n  \t  ",
				"kubernetes": {
					"pod_name": "backgroundprocessing-58c69494d9-7xksj"
				}
			}`,
			expectError:   false,
			expectSkipped: true,
		},
		{
			name: "Structured application log with empty message should be skipped",
			logJSON: `{
				"logs": {
					"instant": {
						"epochSecond": 1734243648,
						"nanoOfSecond": 123456789
					},
					"level": "INFO",
					"loggerName": "com.test.Service",
					"thread": "main",
					"message": ""
				},
				"kubernetes": {
					"pod_name": "test-service-123"
				}
			}`,
			expectError:   false,
			expectSkipped: true,
		},
		{
			name: "Container log with valid message should work",
			logJSON: `{
				"@timestamp": "2025-06-12T15:30:33.192086Z",
				"stream": "stdout",
				"log": "This is a valid log message",
				"kubernetes": {
					"pod_name": "backgroundprocessing-58c69494d9-7xksj"
				}
			}`,
			expectError:   false,
			expectSkipped: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := extractor.ExtractLog(tc.logJSON, source)

			if tc.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorMsg)
				assert.Nil(t, result)
			} else if tc.expectSkipped {
				// Skipped messages return nil, nil (no error, no result)
				assert.NoError(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				// Verify it's a valid ApplicationLog
				appLog, ok := result.(*events.ApplicationLog)
				assert.True(t, ok)
				assert.NotEmpty(t, appLog.Message)
			}
		})
	}
}

// Test that demonstrates the fix for Go's nil pointer interface issue
func TestExtractor_NilPointerInterfaceFix(t *testing.T) {
	extractor := NewExtractor()
	source := events.LogSource{Service: "test", Environment: "D1", Subscription: "cp2"}

	testCases := []struct {
		name    string
		logJSON string
	}{
		{
			name: "Container log with empty message returns true nil",
			logJSON: `{
				"@timestamp": "2025-06-12T15:30:33.192086Z",
				"log": "",
				"kubernetes": {"pod_name": "test-pod"}
			}`,
		},
		{
			name: "Structured log with empty message returns true nil",
			logJSON: `{
				"logs": {
					"instant": {"epochSecond": 1734243648, "nanoOfSecond": 123456789},
					"level": "INFO",
					"loggerName": "test.Logger",
					"thread": "main",
					"message": ""
				},
				"kubernetes": {"pod_name": "test-pod"}
			}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := extractor.ExtractLog(tc.logJSON, source)

			// Should return true nil (not a nil pointer wrapped in interface)
			assert.NoError(t, err)
			assert.Nil(t, result)

			// This is the key test: result should be truly nil, not a wrapped nil pointer
			// In Go, this would fail if we returned (*events.ApplicationLog)(nil) as interface{}
			if result != nil {
				t.Errorf("Expected truly nil result, but got: %v (type: %T)", result, result)
			}

			// Additional check: if we somehow got a non-nil interface with nil content,
			// this would catch it
			switch log := result.(type) {
			case *events.ApplicationLog:
				if log == nil {
					t.Error("Got nil *events.ApplicationLog wrapped in non-nil interface{}")
				}
			case *events.HTTPRequestLog:
				if log == nil {
					t.Error("Got nil *events.HTTPRequestLog wrapped in non-nil interface{}")
				}
			}
		})
	}
}

// Test Apache access log format from the error case
func TestExtractor_ApacheAccessLog(t *testing.T) {
	extractor := NewExtractor()

	// Apache access log from the error case
	rawLine := `{"@timestamp":"2025-06-15T18:14:04.948924Z","record_date":"20250615","_p":"F","log":"{\"localServerName\": \"localhost\", \"remoteHost\": \"127.0.0.1\", \"identdUsername\": \"-\", \"remoteUser\": \"-\", \"time\": \"[15/Jun/2025:18:14:04 +0000]\", \"responseTime\": \"0\", \"requestFirstLine\": \"GET /healthz HTTP/1.1\", \"status\": \"204\", \"bytes\": \"-\", \"referer\": \"-\", \"userAgent\": \"kube-probe/1.31\", \"cache status\": \"-\"}","stream":"stdout","time":"2025-06-15T18:14:04.948924301Z","logs":{"identdUsername":"-","localServerName":"localhost","remoteHost":"127.0.0.1","cache status":"-","remoteUser":"-","requestFirstLine":"GET /healthz HTTP/1.1","responseTime":"0","referer":"-","userAgent":"kube-probe/1.31","time":"[15/Jun/2025:18:14:04 +0000]","bytes":"-","status":"204"},"kubernetes":{"docker_id":"a8c06151bf6763f552a0ea2545ab7381d91e705eb2144be97b2b9e5f4de86b9e","pod_name":"apache2-igc-9db94ff4f-xzl59","pod_id":"7934cdee-429f-46a9-93fc-5ca7f4f8900f","host":"aks-guhn66afpt-25077532-vmss00001b","annotations":{"environment":"d1","data-ingest_dynatrace_com_injected":"true","oneagent_dynatrace_com_injected":"true","ae-version":"20250605-222342","dynakube_dynatrace_com_injected":"true","cni_projectcalico_org_podIPs":"10.244.1.16/32","cni_projectcalico_org_containerID":"74bac728fef3044daaf722ddb01dea23aad1774636580bd400ff40ba97699a70","cni_projectcalico_org_podIP":"10.244.1.16/32","fluentbit_io_parser":"mt-apache-ing"},"container_name":"proxy","labels":{"access-kibana":"","service":"loadbalancer","pod-template-hash":"9db94ff4f","scope":"public","tier":"frontend"},"container_hash":"modeltimagerepo.azurecr.io/cb/ingress-apache2@sha256:989d93fe498416915d7e80565f0ed33973c88e91238747fc0b64facb0501ced8","container_image":"modeltimagerepo.azurecr.io/cb/ingress-apache2:20250520-134500","pod_ip":"10.244.1.16","namespace_name":"default"}}`

	source := events.LogSource{
		Service:      "",
		Environment:  "D1",
		Subscription: "cp2",
	}

	result, err := extractor.ExtractLog(rawLine, source)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Should be extracted as HTTP request log
	httpLog, ok := result.(*events.HTTPRequestLog)
	assert.True(t, ok, "Should extract as HTTPRequestLog")
	assert.NotNil(t, httpLog)

	// Verify basic fields
	assert.Equal(t, "GET", httpLog.Method)
	assert.Equal(t, "/healthz", httpLog.Path)
	assert.Equal(t, 204, httpLog.StatusCode)
	assert.Equal(t, "127.0.0.1", httpLog.ClientIP)
	assert.Equal(t, "apache2-igc-9db94ff4f-xzl59", httpLog.PodName)
	assert.Equal(t, int64(0), httpLog.ResponseTimeMs)
	assert.Equal(t, int64(0), httpLog.BytesSent) // "-" bytes should become 0

	// Verify timestamp is parsed correctly from root level
	assert.Greater(t, httpLog.TimestampNanos, int64(0))

	// Should pass validation
	err = extractor.ValidateExtractedLog(httpLog)
	assert.NoError(t, err)
}

// Test proxy log extraction with ExtractProxyLog method
func TestExtractor_ExtractProxyLog(t *testing.T) {
	extractor := NewExtractor()

	// Apache proxy log from the existing test case
	rawLine := `{"@timestamp":"2025-06-15T18:14:04.948924Z","record_date":"20250615","_p":"F","log":"{\"localServerName\": \"localhost\", \"remoteHost\": \"127.0.0.1\", \"identdUsername\": \"-\", \"remoteUser\": \"-\", \"time\": \"[15/Jun/2025:18:14:04 +0000]\", \"responseTime\": \"0\", \"requestFirstLine\": \"GET /healthz HTTP/1.1\", \"status\": \"204\", \"bytes\": \"-\", \"referer\": \"-\", \"userAgent\": \"kube-probe/1.31\", \"cache status\": \"-\"}","stream":"stdout","time":"2025-06-15T18:14:04.948924301Z","logs":{"identdUsername":"-","localServerName":"localhost","remoteHost":"127.0.0.1","cache status":"-","remoteUser":"-","requestFirstLine":"GET /healthz HTTP/1.1","responseTime":"0","referer":"-","userAgent":"kube-probe/1.31","time":"[15/Jun/2025:18:14:04 +0000]","bytes":"-","status":"204"},"kubernetes":{"docker_id":"a8c06151bf6763f552a0ea2545ab7381d91e705eb2144be97b2b9e5f4de86b9e","pod_name":"apache2-igc-9db94ff4f-xzl59","pod_id":"7934cdee-429f-46a9-93fc-5ca7f4f8900f","host":"aks-guhn66afpt-25077532-vmss00001b","annotations":{"environment":"d1","data-ingest_dynatrace_com_injected":"true","oneagent_dynatrace_com_injected":"true","ae-version":"20250605-222342","dynakube_dynatrace_com_injected":"true","cni_projectcalico_org_podIPs":"10.244.1.16/32","cni_projectcalico_org_containerID":"74bac728fef3044daaf722ddb01dea23aad1774636580bd400ff40ba97699a70","cni_projectcalico_org_podIP":"10.244.1.16/32","fluentbit_io_parser":"mt-apache-ing"},"container_name":"proxy","labels":{"access-kibana":"","service":"loadbalancer","pod-template-hash":"9db94ff4f","scope":"public","tier":"frontend"},"container_hash":"modeltimagerepo.azurecr.io/cb/ingress-apache2@sha256:989d93fe498416915d7e80565f0ed33973c88e91238747fc0b64facb0501ced8","container_image":"modeltimagerepo.azurecr.io/cb/ingress-apache2:20250520-134500","pod_ip":"10.244.1.16","namespace_name":"default"}}`

	source := events.LogSource{
		Service:      "apache-proxy",
		Environment:  "D1",
		Subscription: "cp2",
	}

	result, err := extractor.ExtractProxyLog(rawLine, source)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Should be extracted as ProxyLog
	proxyLog, ok := result.(*events.ProxyLog)
	require.True(t, ok, "Expected ProxyLog, got %T", result)

	// Verify proxy-specific fields
	assert.Equal(t, "GET", proxyLog.Method)
	assert.Equal(t, "/healthz", proxyLog.Path)
	assert.Equal(t, 204, proxyLog.StatusCode)
	assert.Equal(t, "127.0.0.1", proxyLog.ClientIP)
	assert.Equal(t, "localhost", proxyLog.LocalServerName)
	assert.Equal(t, "-", proxyLog.RemoteUser)
	assert.Equal(t, "-", proxyLog.Referer)
	assert.Equal(t, "kube-probe/1.31", proxyLog.UserAgent)
	assert.Equal(t, "-", proxyLog.CacheStatus)
	assert.Equal(t, "apache2-igc-9db94ff4f-xzl59", proxyLog.PodName)
	assert.Equal(t, "10.244.1.16", proxyLog.PodIP)
	assert.Equal(t, int64(0), proxyLog.ResponseTimeMs)
	assert.Equal(t, int64(0), proxyLog.BytesSent) // "-" bytes should become 0

	// Verify timestamp is parsed correctly from root level
	assert.Greater(t, proxyLog.TimestampNanos, int64(0))

	// Should pass validation
	err = extractor.ValidateExtractedLog(result)
	assert.NoError(t, err)
}

// Test proxy log extraction with empty container log (should be skipped)
func TestExtractor_ExtractProxyLog_EmptyContainerLog(t *testing.T) {
	extractor := NewExtractor()

	// Container log with empty message from proxy service (the exact error case)
	rawLine := `{"@timestamp":"2025-06-12T14:55:43.277832Z","log":"","kubernetes":{"container_name":"proxy","docker_id":"2031a85753941e1bdd58e49f66724d11fa600f7dca4166a3d147bae32635b1d5","pod_ip":"10.244.5.22","host":"aks-guhn66afpt-25077532-vmss000018","namespace_name":"default","container_hash":"modeltimagerepo.azurecr.io/cb/ingress-apache2@sha256:989d93fe498416915d7e80565f0ed33973c88e91238747fc0b64facb0501ced8","pod_id":"5826df13-aeef-459c-b7bd-a7c366f1a333","labels":{"tier":"frontend","pod-template-hash":"9db94ff4f","scope":"public","access-kibana":"","service":"loadbalancer"},"annotations":{"cni_projectcalico_org_containerID":"0fdd66b4e0f3a0e847efead3874eaebe858f247b2a8788761209179483d80768","cni_projectcalico_org_podIP":"10.244.5.22/32","data-ingest_dynatrace_com_injected":"true","cni_projectcalico_org_podIPs":"10.244.5.22/32","environment":"d1","oneagent_dynatrace_com_injected":"true","ae-version":"20250605-222342","dynakube_dynatrace_com_injected":"true","fluentbit_io_parser":"mt-apache-ing"},"pod_name":"apache2-igc-9db94ff4f-2tjt8","container_image":"modeltimagerepo.azurecr.io/cb/ingress-apache2:20250520-134500"},"_p":"F","time":"2025-06-12T14:55:43.277832797Z","record_date":"20250612","stream":"stdout"}`

	source := events.LogSource{
		Service:      "apache-proxy",
		Environment:  "D1",
		Subscription: "cp2",
	}

	result, err := extractor.ExtractProxyLog(rawLine, source)

	// Should return nil, nil (no error, no result) for empty log messages
	assert.NoError(t, err)
	assert.Nil(t, result)

	// This is the key test: result should be truly nil, not a wrapped nil pointer
	if result != nil {
		t.Errorf("Expected truly nil result for empty log message, but got: %v (type: %T)", result, result)
	}
}

// Test proxy log extraction with mixed log types (Apache access + container logs)
func TestExtractor_ExtractProxyLog_MixedLogTypes(t *testing.T) {
	extractor := NewExtractor()
	source := events.LogSource{Service: "apache-proxy", Environment: "D1", Subscription: "cp2"}

	testCases := []struct {
		name          string
		rawLine       string
		expectedType  string
		expectSkipped bool
	}{
		{
			name:         "Apache access log should return ProxyLog",
			rawLine:      `{"@timestamp":"2025-06-15T18:14:04.948924Z","logs":{"identdUsername":"-","localServerName":"localhost","remoteHost":"127.0.0.1","cache status":"-","remoteUser":"-","requestFirstLine":"GET /healthz HTTP/1.1","responseTime":"0","referer":"-","userAgent":"kube-probe/1.31","time":"[15/Jun/2025:18:14:04 +0000]","bytes":"-","status":"204"},"kubernetes":{"pod_name":"apache2-igc-9db94ff4f-xzl59"}}`,
			expectedType: "*events.ProxyLog",
		},
		{
			name:         "Container log with content should return ApplicationLog",
			rawLine:      `{"@timestamp":"2025-06-12T14:55:43.277832Z","log":"INFO: Starting Apache server","kubernetes":{"container_name":"proxy","pod_name":"apache2-igc-9db94ff4f-2tjt8"},"time":"2025-06-12T14:55:43.277832797Z"}`,
			expectedType: "*events.ApplicationLog",
		},
		{
			name:          "Empty container log should be skipped",
			rawLine:       `{"@timestamp":"2025-06-12T14:55:43.277832Z","log":"","kubernetes":{"container_name":"proxy","pod_name":"apache2-igc-9db94ff4f-2tjt8"},"time":"2025-06-12T14:55:43.277832797Z"}`,
			expectSkipped: true,
		},
		{
			name:          "Whitespace-only container log should be skipped",
			rawLine:       `{"@timestamp":"2025-06-12T14:55:43.277832Z","log":"   \n  \t  ","kubernetes":{"container_name":"proxy","pod_name":"apache2-igc-9db94ff4f-2tjt8"},"time":"2025-06-12T14:55:43.277832797Z"}`,
			expectSkipped: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := extractor.ExtractProxyLog(tc.rawLine, source)

			if tc.expectSkipped {
				assert.NoError(t, err)
				assert.Nil(t, result)
				// Ensure it's truly nil, not a wrapped nil pointer
				if result != nil {
					t.Errorf("Expected truly nil result for skipped message, but got: %v (type: %T)", result, result)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)

				// Check the expected type
				resultType := fmt.Sprintf("%T", result)
				assert.Equal(t, tc.expectedType, resultType)

				// Should pass validation
				err = extractor.ValidateExtractedLog(result)
				assert.NoError(t, err)
			}
		})
	}
}

// Test proxy log extraction with malformed request line (requestFirstLine = "-")
func TestExtractor_ExtractProxyLog_MalformedRequestLine(t *testing.T) {
	extractor := NewExtractor()

	// Apache access log with malformed request line (the exact error case) - with logs structure
	rawLine := `{"@timestamp":"2025-06-12T14:56:40.217676Z","logs":{"localServerName":"_","remoteHost":"179.29.33.176","identdUsername":"-","remoteUser":"-","time":"[12/Jun/2025:14:56:40 +0000]","responseTime":"0","requestFirstLine":"-","status":"408","bytes":"-","referer":"-","userAgent":"-","cache status":"-"},"kubernetes":{"host":"aks-guhn66afpt-25077532-vmss000018","container_hash":"modeltimagerepo.azurecr.io/cb/ingress-apache2@sha256:989d93fe498416915d7e80565f0ed33973c88e91238747fc0b64facb0501ced8","pod_id":"5826df13-aeef-459c-b7bd-a7c366f1a333","namespace_name":"default","container_image":"modeltimagerepo.azurecr.io/cb/ingress-apache2:20250520-134500","pod_name":"apache2-igc-9db94ff4f-2tjt8","labels":{"scope":"public","service":"loadbalancer","tier":"frontend","access-kibana":"","pod-template-hash":"9db94ff4f"},"container_name":"proxy","docker_id":"2031a85753941e1bdd58e49f66724d11fa600f7dca4166a3d147bae32635b1d5","annotations":{"cni_projectcalico_org_podIPs":"10.244.5.22/32","data-ingest_dynatrace_com_injected":"true","cni_projectcalico_org_containerID":"74bac728fef3044daaf722ddb01dea23aad1774636580bd400ff40ba97699a70","fluentbit_io_parser":"mt-apache-ing","environment":"d1","oneagent_dynatrace_com_injected":"true","dynakube_dynatrace_com_injected":"true"}}}`

	source := events.LogSource{
		Service:      "apache-proxy",
		Environment:  "D1",
		Subscription: "cp2",
	}

	result, err := extractor.ExtractProxyLog(rawLine, source)

	// Should return nil, nil (no error, no result) for malformed requests
	assert.NoError(t, err)
	assert.Nil(t, result)

	// This is the key test: result should be truly nil, not a wrapped nil pointer
	if result != nil {
		t.Errorf("Expected truly nil result for malformed request line, but got: %v (type: %T)", result, result)
	}
}

// Test proxy log extraction with non-standard HTTP method (SSTP_DUPLEX_POST)
func TestExtractor_ExtractProxyLog_NonStandardMethod(t *testing.T) {
	extractor := NewExtractor()

	// Apache access log with non-standard HTTP method (the exact error case from user)
	rawLine := `{"@timestamp":"2025-06-15T02:33:13.242754Z","_p":"F","record_date":"20250615","stream":"stdout","log":"{\"localServerName\": \"_\", \"remoteHost\": \"143.198.75.35\", \"identdUsername\": \"-\", \"remoteUser\": \"-\", \"time\": \"[15/Jun/2025:02:33:13 +0000]\", \"responseTime\": \"0\", \"requestFirstLine\": \"SSTP_DUPLEX_POST /sra_{BA195980-CD49-458b-9E23-C84EE0ADCD75}/ HTTP/1.1\", \"status\": \"400\", \"bytes\": \"226\", \"referer\": \"-\", \"userAgent\": \"-\", \"cache status\": \"-\"}","time":"2025-06-15T02:33:13.242754189Z","logs":{"requestFirstLine":"SSTP_DUPLEX_POST /sra_{BA195980-CD49-458b-9E23-C84EE0ADCD75}/ HTTP/1.1","bytes":"226","referer":"-","userAgent":"-","localServerName":"_","remoteHost":"143.198.75.35","cache status":"-","time":"[15/Jun/2025:02:33:13 +0000]","remoteUser":"-","identdUsername":"-","responseTime":"0","status":"400"},"kubernetes":{"container_name":"proxy","namespace_name":"default","pod_id":"7934cdee-429f-46a9-93fc-5ca7f4f8900f","pod_ip":"10.244.1.16","container_image":"modeltimagerepo.azurecr.io/cb/ingress-apache2:20250520-134500","pod_name":"apache2-igc-9db94ff4f-xzl59","container_hash":"modeltimagerepo.azurecr.io/cb/ingress-apache2@sha256:989d93fe498416915d7e80565f0ed33973c88e91238747fc0b64facb0501ced8","docker_id":"a8c06151bf6763f552a0ea2545ab7381d91e705eb2144be97b2b9e5f4de86b9e","host":"aks-guhn66afpt-25077532-vmss00001b","labels":{"scope":"public","tier":"frontend","pod-template-hash":"9db94ff4f","service":"loadbalancer","access-kibana":""},"annotations":{"cni_projectcalico_org_podIPs":"10.244.1.16/32","data-ingest_dynatrace_com_injected":"true","cni_projectcalico_org_containerID":"74bac728fef3044daaf722ddb01dea23aad1774636580bd400ff40ba97699a70","fluentbit_io_parser":"mt-apache-ing","environment":"d1","cni_projectcalico_org_podIP":"10.244.1.16/32","oneagent_dynatrace_com_injected":"true","ae-version":"20250605-222342","dynakube_dynatrace_com_injected":"true"}}}`

	source := events.LogSource{
		Service:      "apache-proxy",
		Environment:  "D1",
		Subscription: "cp2",
	}

	result, err := extractor.ExtractProxyLog(rawLine, source)

	// Should return nil, nil (no error, no result) for non-standard HTTP methods
	assert.NoError(t, err)
	assert.Nil(t, result)

	// This is the key test: result should be truly nil, not a wrapped nil pointer
	if result != nil {
		t.Errorf("Expected truly nil result for non-standard HTTP method, but got: %v (type: %T)", result, result)
	}
}

// Test HTTP request log extraction with non-standard HTTP method (should also be skipped)
func TestExtractor_ExtractLog_NonStandardMethod(t *testing.T) {
	extractor := NewExtractor()

	// Apache access log with non-standard HTTP method using ExtractLog method
	rawLine := `{"@timestamp":"2025-06-15T02:33:13.242754Z","logs":{"requestFirstLine":"SSTP_DUPLEX_POST /sra_{BA195980-CD49-458b-9E23-C84EE0ADCD75}/ HTTP/1.1","bytes":"226","referer":"-","userAgent":"-","localServerName":"_","remoteHost":"143.198.75.35","cache status":"-","time":"[15/Jun/2025:02:33:13 +0000]","remoteUser":"-","identdUsername":"-","responseTime":"0","status":"400"},"kubernetes":{"pod_name":"apache2-igc-9db94ff4f-xzl59"}}`

	source := events.LogSource{
		Service:      "apache-proxy",
		Environment:  "D1",
		Subscription: "cp2",
	}

	result, err := extractor.ExtractLog(rawLine, source)

	// Should return nil, nil (no error, no result) for non-standard HTTP methods
	assert.NoError(t, err)
	assert.Nil(t, result)

	// This is the key test: result should be truly nil, not a wrapped nil pointer
	if result != nil {
		t.Errorf("Expected truly nil result for non-standard HTTP method, but got: %v (type: %T)", result, result)
	}
}

// Test proxy log extraction with RTSP protocol (non-HTTP protocol)
func TestExtractor_ExtractProxyLog_RTSPProtocol(t *testing.T) {
	extractor := NewExtractor()

	// Apache access log with RTSP protocol (the exact error case from user)
	rawLine := `{"@timestamp":"2025-06-12T18:48:45.398543Z","time":"2025-06-12T18:48:45.398543062Z","logs":{"remoteHost":"172.235.173.150","identdUsername":"-","remoteUser":"-","responseTime":"0","requestFirstLine":"OPTIONS / RTSP/1.0","time":"[12/Jun/2025:18:48:45 +0000]","cache status":"-","bytes":"226","status":"400","referer":"-","userAgent":"-","localServerName":"_"},"record_date":"20250612","log":"{\"localServerName\": \"_\", \"remoteHost\": \"172.235.173.150\", \"identdUsername\": \"-\", \"remoteUser\": \"-\", \"time\": \"[12/Jun/2025:18:48:45 +0000]\", \"responseTime\": \"0\", \"requestFirstLine\": \"OPTIONS / RTSP/1.0\", \"status\": \"400\", \"bytes\": \"226\", \"referer\": \"-\", \"userAgent\": \"-\", \"cache status\": \"-\"}","stream":"stdout","_p":"F","kubernetes":{"pod_name":"apache2-igc-9db94ff4f-2tjt8","namespace_name":"default","host":"aks-guhn66afpt-25077532-vmss000018","annotations":{"cni_projectcalico_org_podIPs":"10.244.5.22/32","oneagent_dynatrace_com_injected":"true","data-ingest_dynatrace_com_injected":"true","dynakube_dynatrace_com_injected":"true","cni_projectcalico_org_containerID":"0fdd66b4e0f3a0e847efead3874eaebe858f247b2a8788761209179483d80768","environment":"d1","fluentbit_io_parser":"mt-apache-ing","cni_projectcalico_org_podIP":"10.244.5.22/32","ae-version":"20250605-222342"},"docker_id":"2031a85753941e1bdd58e49f66724d11fa600f7dca4166a3d147bae32635b1d5","pod_ip":"10.244.5.22","pod_id":"5826df13-aeef-459c-b7bd-a7c366f1a333","labels":{"tier":"frontend","pod-template-hash":"9db94ff4f","access-kibana":"","scope":"public","service":"loadbalancer"},"container_name":"proxy","container_image":"modeltimagerepo.azurecr.io/cb/ingress-apache2:20250520-134500","container_hash":"modeltimagerepo.azurecr.io/cb/ingress-apache2@sha256:989d93fe498416915d7e80565f0ed33973c88e91238747fc0b64facb0501ced8"}}`

	source := events.LogSource{
		Service:      "apache-proxy",
		Environment:  "D1",
		Subscription: "cp2",
	}

	result, err := extractor.ExtractProxyLog(rawLine, source)

	// Should return nil, nil (no error, no result) for non-HTTP protocols
	assert.NoError(t, err)
	assert.Nil(t, result)

	// This is the key test: result should be truly nil, not a wrapped nil pointer
	if result != nil {
		t.Errorf("Expected truly nil result for non-HTTP protocol, but got: %v (type: %T)", result, result)
	}
}

// Test HTTP request log extraction with RTSP protocol (should also be skipped)
func TestExtractor_ExtractLog_RTSPProtocol(t *testing.T) {
	extractor := NewExtractor()

	// Apache access log with RTSP protocol using ExtractLog method
	rawLine := `{"@timestamp":"2025-06-12T18:48:45.398543Z","logs":{"remoteHost":"172.235.173.150","identdUsername":"-","remoteUser":"-","responseTime":"0","requestFirstLine":"OPTIONS / RTSP/1.0","time":"[12/Jun/2025:18:48:45 +0000]","cache status":"-","bytes":"226"},"kubernetes":{"pod_name":"apache2-igc-9db94ff4f-2tjt8"}}`

	source := events.LogSource{
		Service:      "apache-proxy",
		Environment:  "D1",
		Subscription: "cp2",
	}

	result, err := extractor.ExtractLog(rawLine, source)

	// Should return nil, nil (no error, no result) for non-HTTP protocols
	assert.NoError(t, err)
	assert.Nil(t, result)

	// This is the key test: result should be truly nil, not a wrapped nil pointer
	if result != nil {
		t.Errorf("Expected truly nil result for non-HTTP protocol, but got: %v (type: %T)", result, result)
	}
}

// Test isStandardHTTPMethod function
func TestExtractor_IsStandardHTTPMethod(t *testing.T) {
	extractor := NewExtractor()

	testCases := []struct {
		method   string
		expected bool
	}{
		{"GET", true},
		{"POST", true},
		{"PUT", true},
		{"DELETE", true},
		{"HEAD", true},
		{"OPTIONS", true},
		{"PATCH", true},
		{"TRACE", true},
		{"CONNECT", true},
		{"SSTP_DUPLEX_POST", false},
		{"INVALID_METHOD", false},
		{"get", false}, // case sensitive
		{"", false},
	}

	for _, tc := range testCases {
		t.Run(tc.method, func(t *testing.T) {
			result := extractor.isStandardHTTPMethod(tc.method)
			assert.Equal(t, tc.expected, result, "Method: %s", tc.method)
		})
	}
}
