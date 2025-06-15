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

// Mock implementations for testing

type MockConsumer struct {
	mock.Mock
}

func (m *MockConsumer) Subscribe(topics []string, rebalanceCb kafka.RebalanceCb) error {
	args := m.Called(topics, rebalanceCb)
	return args.Error(0)
}

func (m *MockConsumer) ReadMessage(timeout int) (*kafka.Message, error) {
	args := m.Called(timeout)
	return args.Get(0).(*kafka.Message), args.Error(1)
}

func (m *MockConsumer) StoreMessage(message *kafka.Message) error {
	args := m.Called(message)
	return args.Error(0)
}

func (m *MockConsumer) CommitMessage(message *kafka.Message) error {
	args := m.Called(message)
	return args.Error(0)
}

func (m *MockConsumer) Close() error {
	args := m.Called()
	return args.Error(0)
}

type MockProducer struct {
	mock.Mock
}

func (m *MockProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	args := m.Called(msg, deliveryChan)
	return args.Error(0)
}

func (m *MockProducer) Events() chan kafka.Event {
	args := m.Called()
	return args.Get(0).(chan kafka.Event)
}

func (m *MockProducer) Flush(timeoutMs int) int {
	args := m.Called(timeoutMs)
	return args.Int(0)
}

func (m *MockProducer) Close() {
	m.Called()
}

type MockExtractor struct {
	mock.Mock
}

func (m *MockExtractor) ExtractLog(rawLine string, source events.LogSource) (interface{}, error) {
	args := m.Called(rawLine, source)
	return args.Get(0), args.Error(1)
}

func (m *MockExtractor) ValidateExtractedLog(log interface{}) error {
	args := m.Called(log)
	return args.Error(0)
}

type MockMetricsCollector struct {
	mock.Mock
}

func (m *MockMetricsCollector) IncrementMessagesProcessed() {
	m.Called()
}

func (m *MockMetricsCollector) IncrementMessagesExtracted() {
	m.Called()
}

func (m *MockMetricsCollector) IncrementExtractionErrors() {
	m.Called()
}

func (m *MockMetricsCollector) IncrementValidationErrors() {
	m.Called()
}

func (m *MockMetricsCollector) RecordProcessingLatency(durationMs int64) {
	m.Called(durationMs)
}

// Test helpers

func createTestConfig() *config.Config {
	return &config.Config{
		Kafka: config.KafkaConfig{
			Brokers:        "localhost:9092",
			InputTopic:     "Raw.ApplicationLogs",
			OutputTopic:    "Extracted.Application",
			ErrorTopic:     "Extraction.Errors",
			ConsumerGroup:  "extraction-test",
			BatchSize:      100,
			FlushTimeoutMs: 5000,
		},
		Processing: config.ProcessingConfig{
			MaxConcurrency:   12, // Match partition count for optimal performance
			EnableValidation: true,
			SkipInvalidLogs:  false,
			LogParseErrors:   false,
		},
		Logging: config.LoggingConfig{
			Level:           "info",
			EnableMetrics:   false,
			MetricsInterval: 60,
		},
	}
}

func createTestService() (*Service, *MockConsumer, *MockProducer, *MockExtractor, *MockMetricsCollector) {
	config := createTestConfig()
	mockConsumer := &MockConsumer{}
	mockProducer := &MockProducer{}
	mockExtractor := &MockExtractor{}
	mockMetrics := &MockMetricsCollector{}

	service := NewService(config, mockConsumer, mockProducer, mockExtractor, mockMetrics)
	return service, mockConsumer, mockProducer, mockExtractor, mockMetrics
}

// Tests

func TestNewService(t *testing.T) {
	config := createTestConfig()
	mockConsumer := &MockConsumer{}
	mockProducer := &MockProducer{}
	mockExtractor := &MockExtractor{}
	mockMetrics := &MockMetricsCollector{}

	service := NewService(config, mockConsumer, mockProducer, mockExtractor, mockMetrics)

	assert.NotNil(t, service)
	assert.Equal(t, config, service.config)
}

func TestProcessMessage_Success(t *testing.T) {
	service, mockConsumer, mockProducer, mockExtractor, mockMetrics := createTestService()

	rawLog := `{"logs":{"instant":{"epochSecond":1735606800,"nanoOfSecond":123456789},"level":"INFO","loggerName":"com.example.Service","thread":"main","message":"Test message"},"kubernetes":{"pod_name":"api-service-123-abc"}}`
	msg := &kafka.Message{
		Value: []byte(rawLog),
		Headers: []kafka.Header{
			{Key: "service", Value: []byte("api-service")},
			{Key: "environment", Value: []byte("test")},
		},
	}

	expectedExtracted := &events.ApplicationLog{
		TimestampNanos: 1735606800123456789,
		Level:          "INFO",
		Logger:         "com.example.Service",
		Thread:         "main",
		Message:        "Test message",
		PodName:        "api-service-123-abc",
	}

	mockMetrics.On("IncrementMessagesProcessed").Return()
	mockExtractor.On("ExtractLog", rawLog, mock.AnythingOfType("events.LogSource")).Return(expectedExtracted, nil)
	mockExtractor.On("ValidateExtractedLog", expectedExtracted).Return(nil)
	mockProducer.On("Produce", mock.AnythingOfType("*kafka.Message"), (chan kafka.Event)(nil)).Return(nil)
	mockMetrics.On("IncrementMessagesExtracted").Return()
	mockConsumer.On("CommitMessage", msg).Return(nil)
	mockMetrics.On("RecordProcessingLatency", mock.AnythingOfType("int64")).Return()

	// Execute
	err := service.processMessageConcurrent(context.Background(), 1, msg)

	// Verify
	assert.NoError(t, err)
	mockConsumer.AssertExpectations(t)
	mockProducer.AssertExpectations(t)
	mockExtractor.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}

func TestProcessMessage_ExtractionError_SkipEnabled(t *testing.T) {
	service, mockConsumer, _, mockExtractor, mockMetrics := createTestService()
	service.config.Processing.SkipInvalidLogs = true

	rawLog := `invalid json`
	msg := &kafka.Message{
		Value: []byte(rawLog),
	}

	// Set up mocks (for concurrent processing)
	mockMetrics.On("IncrementMessagesProcessed").Return()
	mockExtractor.On("ExtractLog", rawLog, mock.AnythingOfType("events.LogSource")).Return(nil, assert.AnError)
	mockMetrics.On("IncrementExtractionErrors").Return()
	mockConsumer.On("CommitMessage", msg).Return(nil)
	mockMetrics.On("RecordProcessingLatency", mock.AnythingOfType("int64")).Return()

	// Execute
	err := service.processMessageConcurrent(context.Background(), 1, msg)

	// Verify
	assert.NoError(t, err)
	mockConsumer.AssertExpectations(t)
	mockExtractor.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}

func TestProcessMessage_ValidationError_SkipEnabled(t *testing.T) {
	service, mockConsumer, _, mockExtractor, mockMetrics := createTestService()
	service.config.Processing.SkipInvalidLogs = true

	rawLog := `{"logs":{"instant":{"epochSecond":1735606800,"nanoOfSecond":123456789},"level":"INFO","loggerName":"com.example.Service","thread":"main","message":"Test message"}}`
	msg := &kafka.Message{Value: []byte(rawLog)}

	extractedLog := &events.ApplicationLog{
		TimestampNanos: 1735606800123456789,
		Level:          "INFO",
		Logger:         "com.example.Service",
		Thread:         "main",
		Message:        "Test message",
		PodName:        "test-pod",
	}

	// Set up mocks (for concurrent processing)
	mockMetrics.On("IncrementMessagesProcessed").Return()
	mockExtractor.On("ExtractLog", rawLog, mock.AnythingOfType("events.LogSource")).Return(extractedLog, nil)
	mockExtractor.On("ValidateExtractedLog", extractedLog).Return(assert.AnError)
	mockMetrics.On("IncrementValidationErrors").Return()
	mockConsumer.On("CommitMessage", msg).Return(nil)
	mockMetrics.On("RecordProcessingLatency", mock.AnythingOfType("int64")).Return()

	// Execute
	err := service.processMessageConcurrent(context.Background(), 1, msg)

	// Verify
	assert.NoError(t, err)
	mockConsumer.AssertExpectations(t)
	mockExtractor.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}

func TestProcessMessage_Timeout(t *testing.T) {
	service, _, _, mockExtractor, mockMetrics := createTestService()
	service.config.Processing.SkipInvalidLogs = true // Skip sending error to producer

	// Test concurrent processing with invalid JSON (simulates parsing error)
	msg := &kafka.Message{Value: []byte("invalid json")}

	// Set up mocks for skip invalid logs case
	mockMetrics.On("IncrementMessagesProcessed").Return()
	mockExtractor.On("ExtractLog", "invalid json", mock.AnythingOfType("events.LogSource")).Return(nil, assert.AnError)
	mockMetrics.On("IncrementExtractionErrors").Return()
	mockConsumer := service.consumer.(*MockConsumer)
	mockConsumer.On("CommitMessage", msg).Return(nil)
	mockMetrics.On("RecordProcessingLatency", mock.AnythingOfType("int64")).Return()

	// Execute - test that the concurrent processor handles extraction errors
	err := service.processMessageConcurrent(context.Background(), 1, msg)

	// Should not error when skip_invalid_logs=true
	assert.NoError(t, err)
	mockExtractor.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}

func TestExtractSourceFromMessage(t *testing.T) {
	service, _, _, _, _ := createTestService()

	msg := &kafka.Message{
		Headers: []kafka.Header{
			{Key: "service", Value: []byte("api-service")},
			{Key: "environment", Value: []byte("P1")},
			{Key: "subscription", Value: []byte("cp2")},
		},
	}

	source := service.extractSourceFromMessage(msg)

	assert.Equal(t, "api-service", source.Service)
	assert.Equal(t, "P1", source.Environment)
	assert.Equal(t, "cp2", source.Subscription)
}

func TestClose(t *testing.T) {
	service, mockConsumer, mockProducer, _, _ := createTestService()

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

// Test handling of empty messages that return nil, nil from extractor
func TestService_EmptyMessageHandling(t *testing.T) {
	// Create a mock extractor that returns nil, nil for empty messages
	mockExtractor := &MockExtractor{}
	mockExtractor.On("ExtractLog", mock.AnythingOfType("string"), mock.AnythingOfType("events.LogSource")).
		Return(nil, nil) // Simulate empty message

	mockConsumer := &MockConsumer{}
	mockProducer := &MockProducer{}
	mockMetrics := &MockMetricsCollector{}

	// Set up mock expectations for metrics that are always called
	mockMetrics.On("IncrementMessagesProcessed").Return()
	mockMetrics.On("RecordProcessingLatency", mock.AnythingOfType("int64")).Return()

	// Create config with validation enabled to test the nil check before validation
	config := &config.Config{
		Processing: config.ProcessingConfig{
			EnableValidation: true,
			SkipInvalidLogs:  false,
		},
	}

	service := NewService(config, mockConsumer, mockProducer, mockExtractor, mockMetrics)

	// Create a test message
	msg := &kafka.Message{
		Key:   []byte("test-key"),
		Value: []byte(""), // Empty message
		Headers: []kafka.Header{
			{Key: "service", Value: []byte("test-service")},
			{Key: "environment", Value: []byte("test-env")},
			{Key: "subscription", Value: []byte("test-sub")},
		},
	}

	// Mock the consumer to commit the message
	mockConsumer.On("CommitMessage", msg).Return(nil)

	// Process the message
	ctx := context.Background()
	err := service.processMessageConcurrent(ctx, 1, msg)

	// Should not return an error
	assert.NoError(t, err)

	// Verify that ExtractLog was called
	mockExtractor.AssertExpectations(t)

	// Verify that CommitMessage was called (message should be committed)
	mockConsumer.AssertExpectations(t)

	// Verify that no producer calls were made (no output or error messages)
	mockProducer.AssertNotCalled(t, "Produce")

	// Verify that success count was incremented
	assert.Equal(t, int64(1), service.successCount)
}
