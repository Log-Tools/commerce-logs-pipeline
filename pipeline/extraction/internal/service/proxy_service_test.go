package service

import (
	"context"
	"testing"

	"pipeline/events"
	"pipeline/extraction/internal/config"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Test helpers for proxy service

func createTestProxyConfig() *config.Config {
	return &config.Config{
		Kafka: config.KafkaConfig{
			Brokers:            "localhost:9092",
			ProxyTopic:         "Raw.ProxyLogs",
			ProxyOutputTopic:   "Extracted.Proxy",
			ErrorTopic:         "Extraction.Errors",
			ProxyConsumerGroup: "extraction-proxy-test",
			BatchSize:          100,
			FlushTimeoutMs:     5000,
		},
		Processing: config.ProcessingConfig{
			ProxyMaxConcurrency: 12,
			EnableValidation:    true,
			SkipInvalidLogs:     false,
			LogParseErrors:      false,
		},
		Logging: config.LoggingConfig{
			Level:           "info",
			EnableMetrics:   false,
			MetricsInterval: 60,
		},
	}
}

func createTestProxyService() (*ProxyService, *MockConsumer, *MockProducer, *MockExtractor, *MockMetricsCollector) {
	config := createTestProxyConfig()
	mockConsumer := &MockConsumer{}
	mockProducer := &MockProducer{}
	mockExtractor := &MockExtractor{}
	mockMetrics := &MockMetricsCollector{}

	service := NewProxyService(config, mockConsumer, mockProducer, mockExtractor, mockMetrics)
	return service, mockConsumer, mockProducer, mockExtractor, mockMetrics
}

// Tests

func TestNewProxyService(t *testing.T) {
	config := createTestProxyConfig()
	mockConsumer := &MockConsumer{}
	mockProducer := &MockProducer{}
	mockExtractor := &MockExtractor{}
	mockMetrics := &MockMetricsCollector{}

	service := NewProxyService(config, mockConsumer, mockProducer, mockExtractor, mockMetrics)

	assert.NotNil(t, service)
	assert.Equal(t, config, service.config)
}

func TestProxyService_ProcessMessage_ApacheAccessLog(t *testing.T) {
	service, mockConsumer, mockProducer, mockExtractor, mockMetrics := createTestProxyService()

	// Apache access log from proxy service
	rawLog := `{"@timestamp":"2025-06-15T18:14:04.948924Z","logs":{"identdUsername":"-","localServerName":"localhost","remoteHost":"127.0.0.1","cache status":"-","remoteUser":"-","requestFirstLine":"GET /healthz HTTP/1.1","responseTime":"0","referer":"-","userAgent":"kube-probe/1.31","time":"[15/Jun/2025:18:14:04 +0000]","bytes":"-","status":"204"},"kubernetes":{"pod_name":"apache2-igc-9db94ff4f-xzl59"}}`
	msg := &kafka.Message{
		Value: []byte(rawLog),
		Headers: []kafka.Header{
			{Key: "service", Value: []byte("apache-proxy")},
			{Key: "environment", Value: []byte("D1")},
			{Key: "subscription", Value: []byte("cp2")},
		},
	}

	expectedExtracted := &events.ProxyLog{
		TimestampNanos:  1750015444948924000,
		Method:          "GET",
		Path:            "/healthz",
		StatusCode:      204,
		ResponseTimeMs:  0,
		BytesSent:       0,
		ClientIP:        "127.0.0.1",
		LocalServerName: "localhost",
		RemoteUser:      "-",
		Referer:         "-",
		UserAgent:       "kube-probe/1.31",
		CacheStatus:     "-",
		PodName:         "apache2-igc-9db94ff4f-xzl59",
	}

	mockMetrics.On("IncrementMessagesProcessed").Return()
	mockExtractor.On("ExtractProxyLog", rawLog, mock.AnythingOfType("events.LogSource")).Return(expectedExtracted, nil)
	mockExtractor.On("ValidateExtractedLog", expectedExtracted).Return(nil)
	mockProducer.On("Produce", mock.AnythingOfType("*kafka.Message"), (chan kafka.Event)(nil)).Return(nil)
	mockMetrics.On("IncrementMessagesExtracted").Return()
	mockConsumer.On("CommitMessage", msg).Return(nil)
	mockMetrics.On("RecordProcessingLatency", mock.AnythingOfType("int64")).Return()

	// Execute
	err := service.processProxyMessageConcurrent(context.Background(), 1, msg)

	// Verify
	assert.NoError(t, err)
	mockConsumer.AssertExpectations(t)
	mockProducer.AssertExpectations(t)
	mockExtractor.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}

func TestProxyService_ProcessMessage_ContainerLog(t *testing.T) {
	service, mockConsumer, mockProducer, mockExtractor, mockMetrics := createTestProxyService()

	// Container log from proxy service
	rawLog := `{"@timestamp":"2025-06-12T14:55:43.277832Z","log":"INFO: Starting Apache server","kubernetes":{"container_name":"proxy","pod_name":"apache2-igc-9db94ff4f-2tjt8"},"time":"2025-06-12T14:55:43.277832797Z"}`
	msg := &kafka.Message{
		Value: []byte(rawLog),
		Headers: []kafka.Header{
			{Key: "service", Value: []byte("apache-proxy")},
			{Key: "environment", Value: []byte("D1")},
			{Key: "subscription", Value: []byte("cp2")},
		},
	}

	expectedExtracted := &events.ApplicationLog{
		TimestampNanos: 1749740143277832797,
		Level:          "INFO",
		Logger:         "apache2-igc",
		Thread:         "",
		Message:        "INFO: Starting Apache server",
		Thrown:         nil,
		ClientIP:       "",
		PodName:        "apache2-igc-9db94ff4f-2tjt8",
	}

	mockMetrics.On("IncrementMessagesProcessed").Return()
	mockExtractor.On("ExtractProxyLog", rawLog, mock.AnythingOfType("events.LogSource")).Return(expectedExtracted, nil)
	mockExtractor.On("ValidateExtractedLog", expectedExtracted).Return(nil)
	mockProducer.On("Produce", mock.AnythingOfType("*kafka.Message"), (chan kafka.Event)(nil)).Return(nil)
	mockMetrics.On("IncrementMessagesExtracted").Return()
	mockConsumer.On("CommitMessage", msg).Return(nil)
	mockMetrics.On("RecordProcessingLatency", mock.AnythingOfType("int64")).Return()

	// Execute
	err := service.processProxyMessageConcurrent(context.Background(), 1, msg)

	// Verify
	assert.NoError(t, err)
	mockConsumer.AssertExpectations(t)
	mockProducer.AssertExpectations(t)
	mockExtractor.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}

// Test the exact error case: empty container log should be skipped
func TestProxyService_ProcessMessage_EmptyContainerLog(t *testing.T) {
	service, mockConsumer, _, mockExtractor, mockMetrics := createTestProxyService()

	// Empty container log from proxy service (the exact error case)
	rawLog := `{"@timestamp":"2025-06-12T14:55:43.277832Z","log":"","kubernetes":{"container_name":"proxy","docker_id":"2031a85753941e1bdd58e49f66724d11fa600f7dca4166a3d147bae32635b1d5","pod_ip":"10.244.5.22","host":"aks-guhn66afpt-25077532-vmss000018","namespace_name":"default","container_hash":"modeltimagerepo.azurecr.io/cb/ingress-apache2@sha256:989d93fe498416915d7e80565f0ed33973c88e91238747fc0b64facb0501ced8","pod_id":"5826df13-aeef-459c-b7bd-a7c366f1a333","labels":{"tier":"frontend","pod-template-hash":"9db94ff4f","scope":"public","access-kibana":"","service":"loadbalancer"},"annotations":{"cni_projectcalico_org_containerID":"0fdd66b4e0f3a0e847efead3874eaebe858f247b2a8788761209179483d80768","cni_projectcalico_org_podIP":"10.244.5.22/32","data-ingest_dynatrace_com_injected":"true","cni_projectcalico_org_podIPs":"10.244.5.22/32","environment":"d1","oneagent_dynatrace_com_injected":"true","ae-version":"20250605-222342","dynakube_dynatrace_com_injected":"true","fluentbit_io_parser":"mt-apache-ing"},"pod_name":"apache2-igc-9db94ff4f-2tjt8","container_image":"modeltimagerepo.azurecr.io/cb/ingress-apache2:20250520-134500"},"_p":"F","time":"2025-06-12T14:55:43.277832797Z","record_date":"20250612","stream":"stdout"}`
	msg := &kafka.Message{
		Value: []byte(rawLog),
		Headers: []kafka.Header{
			{Key: "service", Value: []byte("apache-proxy")},
			{Key: "environment", Value: []byte("D1")},
			{Key: "subscription", Value: []byte("cp2")},
		},
	}

	// Set up mocks - extractor should return nil, nil for empty messages
	mockMetrics.On("IncrementMessagesProcessed").Return()
	mockExtractor.On("ExtractProxyLog", rawLog, mock.AnythingOfType("events.LogSource")).Return(nil, nil)
	mockConsumer.On("CommitMessage", msg).Return(nil)
	mockMetrics.On("RecordProcessingLatency", mock.AnythingOfType("int64")).Return()

	// Execute
	err := service.processProxyMessageConcurrent(context.Background(), 1, msg)

	// Verify - should not return an error
	assert.NoError(t, err)
	mockConsumer.AssertExpectations(t)
	mockExtractor.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)

	// Verify that no producer calls were made (no output or error messages)
	mockProducer := service.producer.(*MockProducer)
	mockProducer.AssertNotCalled(t, "Produce")

	// Verify that success count was incremented (empty messages count as successful)
	assert.Equal(t, int64(1), service.successCount)
}

func TestProxyService_ProcessMessage_ExtractionError_SkipEnabled(t *testing.T) {
	service, mockConsumer, _, mockExtractor, mockMetrics := createTestProxyService()
	service.config.Processing.SkipInvalidLogs = true

	rawLog := `invalid json`
	msg := &kafka.Message{
		Value: []byte(rawLog),
		Headers: []kafka.Header{
			{Key: "service", Value: []byte("apache-proxy")},
			{Key: "environment", Value: []byte("D1")},
			{Key: "subscription", Value: []byte("cp2")},
		},
	}

	// Set up mocks
	mockMetrics.On("IncrementMessagesProcessed").Return()
	mockExtractor.On("ExtractProxyLog", rawLog, mock.AnythingOfType("events.LogSource")).Return(nil, assert.AnError)
	mockMetrics.On("IncrementExtractionErrors").Return()
	mockConsumer.On("CommitMessage", msg).Return(nil)
	mockMetrics.On("RecordProcessingLatency", mock.AnythingOfType("int64")).Return()

	// Execute
	err := service.processProxyMessageConcurrent(context.Background(), 1, msg)

	// Verify
	assert.NoError(t, err)
	mockConsumer.AssertExpectations(t)
	mockExtractor.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}

func TestProxyService_ProcessMessage_ValidationError_SkipEnabled(t *testing.T) {
	service, mockConsumer, _, mockExtractor, mockMetrics := createTestProxyService()
	service.config.Processing.SkipInvalidLogs = true

	rawLog := `{"@timestamp":"2025-06-15T18:14:04.948924Z","logs":{"requestFirstLine":"GET /healthz HTTP/1.1","status":"204"},"kubernetes":{"pod_name":"apache2-igc-9db94ff4f-xzl59"}}`
	msg := &kafka.Message{Value: []byte(rawLog)}

	extractedLog := &events.ProxyLog{
		TimestampNanos: 1750015444948924000,
		Method:         "GET",
		Path:           "/healthz",
		StatusCode:     204,
		PodName:        "apache2-igc-9db94ff4f-xzl59",
	}

	// Set up mocks
	mockMetrics.On("IncrementMessagesProcessed").Return()
	mockExtractor.On("ExtractProxyLog", rawLog, mock.AnythingOfType("events.LogSource")).Return(extractedLog, nil)
	mockExtractor.On("ValidateExtractedLog", extractedLog).Return(assert.AnError)
	mockMetrics.On("IncrementValidationErrors").Return()
	mockConsumer.On("CommitMessage", msg).Return(nil)
	mockMetrics.On("RecordProcessingLatency", mock.AnythingOfType("int64")).Return()

	// Execute
	err := service.processProxyMessageConcurrent(context.Background(), 1, msg)

	// Verify
	assert.NoError(t, err)
	mockConsumer.AssertExpectations(t)
	mockExtractor.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}

func TestProxyService_ExtractSourceFromMessage(t *testing.T) {
	service, _, _, _, _ := createTestProxyService()

	msg := &kafka.Message{
		Headers: []kafka.Header{
			{Key: "service", Value: []byte("apache-proxy")},
			{Key: "environment", Value: []byte("D1")},
			{Key: "subscription", Value: []byte("cp2")},
		},
	}

	source := service.extractSourceFromMessage(msg)

	assert.Equal(t, "apache-proxy", source.Service)
	assert.Equal(t, "D1", source.Environment)
	assert.Equal(t, "cp2", source.Subscription)
}

func TestProxyService_Close(t *testing.T) {
	service, mockConsumer, mockProducer, _, _ := createTestProxyService()

	// Set up mocks
	mockProducer.On("Flush", 5000).Return(0)
	mockProducer.On("Close").Return()
	mockConsumer.On("Close").Return(nil)

	// Execute
	service.Close()

	// Verify
	mockConsumer.AssertExpectations(t)
	mockProducer.AssertExpectations(t)
}

// Test that demonstrates the fix for the nil pointer interface issue in proxy service
func TestProxyService_NilPointerInterfaceFix(t *testing.T) {
	service, mockConsumer, _, mockExtractor, mockMetrics := createTestProxyService()

	testCases := []struct {
		name   string
		rawLog string
	}{
		{
			name:   "Empty container log returns true nil",
			rawLog: `{"@timestamp":"2025-06-12T14:55:43.277832Z","log":"","kubernetes":{"pod_name":"apache2-igc-9db94ff4f-2tjt8"},"time":"2025-06-12T14:55:43.277832797Z"}`,
		},
		{
			name:   "Whitespace-only container log returns true nil",
			rawLog: `{"@timestamp":"2025-06-12T14:55:43.277832Z","log":"   \n  \t  ","kubernetes":{"pod_name":"apache2-igc-9db94ff4f-2tjt8"},"time":"2025-06-12T14:55:43.277832797Z"}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			msg := &kafka.Message{
				Value: []byte(tc.rawLog),
				Headers: []kafka.Header{
					{Key: "service", Value: []byte("apache-proxy")},
					{Key: "environment", Value: []byte("D1")},
					{Key: "subscription", Value: []byte("cp2")},
				},
			}

			// Mock extractor to return true nil (not a nil pointer wrapped in interface)
			mockMetrics.On("IncrementMessagesProcessed").Return()
			mockExtractor.On("ExtractProxyLog", tc.rawLog, mock.AnythingOfType("events.LogSource")).Return(nil, nil)
			mockConsumer.On("CommitMessage", msg).Return(nil)
			mockMetrics.On("RecordProcessingLatency", mock.AnythingOfType("int64")).Return()

			// Process the message
			err := service.processProxyMessageConcurrent(context.Background(), 1, msg)

			// Should not return an error
			assert.NoError(t, err)

			// Verify that ExtractProxyLog was called
			mockExtractor.AssertExpectations(t)

			// Verify that CommitMessage was called (message should be committed)
			mockConsumer.AssertExpectations(t)

			// Verify that no validation was attempted (since result was nil)
			mockExtractor.AssertNotCalled(t, "ValidateExtractedLog")

			// Verify that success count was incremented (count accumulates across test cases)
			assert.Greater(t, service.successCount, int64(0))

			// Reset mocks for next iteration
			mockMetrics.ExpectedCalls = nil
			mockExtractor.ExpectedCalls = nil
			mockConsumer.ExpectedCalls = nil
		})
	}
}
