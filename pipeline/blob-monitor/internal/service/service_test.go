package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blobConfig "github.com/Log-Tools/commerce-logs-pipeline/pipeline/blob-monitor/internal/config"
	"github.com/Log-Tools/commerce-logs-pipeline/pipeline/blob-monitor/internal/events"
)

// MockProducer is a simple mock for testing
type MockProducer struct {
	mutex    sync.RWMutex
	messages []MockMessage
	events   chan kafka.Event
}

type MockMessage struct {
	Topic string
	Key   []byte
	Value []byte
}

func NewMockProducer() *MockProducer {
	return &MockProducer{
		messages: make([]MockMessage, 0),
		events:   make(chan kafka.Event, 100),
	}
}

func (m *MockProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.messages = append(m.messages, MockMessage{
		Topic: *msg.TopicPartition.Topic,
		Key:   msg.Key,
		Value: msg.Value,
	})

	// Send delivery confirmation synchronously for testing
	if deliveryChan != nil {
		deliveryChan <- &kafka.Message{
			TopicPartition: msg.TopicPartition,
			Key:            msg.Key,
			Value:          msg.Value,
		}
	}

	return nil
}

func (m *MockProducer) Events() chan kafka.Event {
	return m.events
}

func (m *MockProducer) Flush(timeoutMs int) int {
	return 0
}

func (m *MockProducer) Close() {}

func (m *MockProducer) GetProducedMessages() []MockMessage {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.messages
}

// MockConsumer is a simple mock for testing
type MockConsumer struct {
	messages []*kafka.Message
	index    int
}

func NewMockConsumer() *MockConsumer {
	return &MockConsumer{
		messages: make([]*kafka.Message, 0),
		index:    0,
	}
}

func (m *MockConsumer) Subscribe(topic string, rebalanceCb kafka.RebalanceCb) error {
	return nil
}

func (m *MockConsumer) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	if m.index >= len(m.messages) {
		return nil, kafka.NewError(kafka.ErrTimedOut, "No more messages", false)
	}

	msg := m.messages[m.index]
	m.index++
	return msg, nil
}

func (m *MockConsumer) Close() error {
	return nil
}

func (m *MockConsumer) AddMessage(topic string, key []byte, value []byte) {
	m.messages = append(m.messages, &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Key:            key,
		Value:          value,
	})
}

// Validates service initialization correctly wires dependencies and internal state
func TestNewBlobMonitorService(t *testing.T) {
	config := createTestConfig()
	mockProducer := NewMockProducer()
	mockStorageFactory := &MockStorageClientFactory{}

	service, err := NewBlobMonitorService(config, mockProducer, mockStorageFactory)
	require.NoError(t, err)

	// Verify service is properly initialized with injected dependencies
	assert.NotNil(t, service)
	assert.Equal(t, config, service.config)
	assert.NotNil(t, service.selectors)
	assert.NotNil(t, service.stopChannel)
}

// Validates individual blob discovery events are correctly serialized and published to Kafka
func TestPublishBlobObservedEvent(t *testing.T) {
	config := createTestConfig()
	mockProducer := NewMockProducer()
	mockStorageFactory := &MockStorageClientFactory{}

	service, err := NewBlobMonitorService(config, mockProducer, mockStorageFactory)
	require.NoError(t, err)

	// Test with apache-proxy blob to verify service selector handling
	event := events.BlobObservedEvent{
		Subscription:     "test-sub",
		Environment:      "test-env",
		BlobName:         "kubernetes/20250607.apache2-igc_proxy-test.gz",
		ObservationDate:  time.Now(),
		SizeInBytes:      1024,
		LastModifiedDate: time.Now(),
		ServiceSelector:  "apache-proxy",
	}

	err = service.publishBlobObservedEvent(event)
	require.NoError(t, err)

	// Verify Kafka message structure and content
	messages := mockProducer.GetProducedMessages()
	require.Len(t, messages, 1, "Expected exactly one message to be published")

	msg := messages[0]
	assert.Equal(t, "test-topic", msg.Topic)
	// Verify key format follows pattern: {subscription}:{environment}:{eventType}:{cleanBlobName}
	assert.Equal(t, "test-sub:test-env:observed:20250607.apache2-igc_proxy-test.gz", string(msg.Key))
	// Verify critical fields are present in JSON payload
	assert.Contains(t, string(msg.Value), "test-sub")
	assert.Contains(t, string(msg.Value), "apache-proxy")
}

// Validates completion events are published with correct aggregation data
func TestPublishBlobsListedEvent(t *testing.T) {
	config := createTestConfig()
	mockProducer := NewMockProducer()
	mockStorageFactory := &MockStorageClientFactory{}

	service, err := NewBlobMonitorService(config, mockProducer, mockStorageFactory)
	require.NoError(t, err)

	// Simulate completion of scanning 5 blobs totaling ~1MB
	event := events.BlobsListedEvent{
		Subscription:     "test-sub",
		Environment:      "test-env",
		ServiceSelector:  "apache-proxy",
		Date:             "20250607",
		ListingStartTime: time.Now(),
		ListingEndTime:   time.Now(),
		BlobCount:        5,
		TotalBytes:       1024000,
	}

	err = service.publishBlobsListedEvent(event)
	require.NoError(t, err)

	// Verify completion event structure and aggregation data
	messages := mockProducer.GetProducedMessages()
	require.Len(t, messages, 1, "Expected exactly one completion event")

	msg := messages[0]
	assert.Equal(t, "test-topic", msg.Topic)
	// Verify key format follows pattern: {subscription}:{environment}:{eventType}:{blobName}
	assert.Equal(t, "test-sub:test-env:listed:apache-proxy-20250607", string(msg.Key))
	// Verify payload contains scanning metadata
	assert.Contains(t, string(msg.Value), "test-sub")
	assert.Contains(t, string(msg.Value), "apache-proxy")
	assert.Contains(t, string(msg.Value), "20250607")
}

// Validates blob closed events are correctly serialized and published to Kafka
func TestPublishBlobClosedEvent(t *testing.T) {
	config := createTestConfig()
	mockProducer := NewMockProducer()
	mockStorageFactory := &MockStorageClientFactory{}

	service, err := NewBlobMonitorService(config, mockProducer, mockStorageFactory)
	require.NoError(t, err)

	// Test BlobClosed event publishing
	event := events.BlobClosedEvent{
		Subscription:     "test-sub",
		Environment:      "test-env",
		BlobName:         "kubernetes/20250607.apache2-igc_proxy-test.gz",
		ServiceSelector:  "apache-proxy",
		LastModifiedDate: time.Now().Add(-10 * time.Minute),
		ClosedDate:       time.Now(),
		SizeInBytes:      2048,
		TimeoutMinutes:   5,
	}

	err = service.PublishBlobClosedEvent(event)
	require.NoError(t, err)

	// Verify Kafka message structure and content
	messages := mockProducer.GetProducedMessages()
	require.Len(t, messages, 1, "Expected exactly one message to be published")

	msg := messages[0]
	assert.Equal(t, "test-topic", msg.Topic)
	// Verify key format follows pattern: {subscription}:{environment}:{eventType}:{cleanBlobName}
	assert.Equal(t, "test-sub:test-env:closed:20250607.apache2-igc_proxy-test.gz", string(msg.Key))
	// Verify critical fields are present in JSON payload
	assert.Contains(t, string(msg.Value), "test-sub")
	assert.Contains(t, string(msg.Value), "apache-proxy")
	assert.Contains(t, string(msg.Value), "closedDate")
	assert.Contains(t, string(msg.Value), "timeoutMinutes")
}

// Validates graceful shutdown closes internal channels and flushes Kafka producer
func TestServiceStop(t *testing.T) {
	config := createTestConfig()
	mockProducer := NewMockProducer()
	mockStorageFactory := &MockStorageClientFactory{}

	service, err := NewBlobMonitorService(config, mockProducer, mockStorageFactory)
	require.NoError(t, err)

	// Verify stop channel is open initially
	select {
	case <-service.stopChannel:
		t.Fatal("Stop channel should not be closed initially")
	default:
		// Expected - channel is open
	}

	// Stop the service
	service.Stop()

	// Verify stop channel is closed
	select {
	case <-service.stopChannel:
		// Expected - channel is now closed
	default:
		t.Fatal("Stop channel should be closed after Stop()")
	}
}

// Validates MockStorageClientFactory provides empty client map for testing
func TestMockStorageClientFactory(t *testing.T) {
	factory := &MockStorageClientFactory{}
	config := createTestConfig()

	clients, err := factory.CreateClients(config)
	require.NoError(t, err)
	assert.NotNil(t, clients)
	assert.Empty(t, clients)
}

// Validates AzureStorageClientFactory interface but skips actual Azure calls in unit tests
func TestAzureStorageClientFactory_Interface(t *testing.T) {
	factory := &AzureStorageClientFactory{}
	config := createTestConfig()

	// This will fail because we don't have real Azure config in tests,
	// but it validates that the interface is implemented correctly
	_, err := factory.CreateClients(config)
	assert.Error(t, err) // Expected to fail without real Azure config
	assert.Contains(t, err.Error(), "failed to get storage account")
}

// Validates that dependency injection eliminates the need for special testing constructors
func TestDependencyInjectionDesign(t *testing.T) {
	config := createTestConfig()
	mockProducer := NewMockProducer()

	// Test with mock factory
	mockFactory := &MockStorageClientFactory{}
	service1, err := NewBlobMonitorService(config, mockProducer, mockFactory)
	require.NoError(t, err)
	assert.NotNil(t, service1)

	// Test with real factory (will fail due to missing Azure config, but proves interface works)
	realFactory := &AzureStorageClientFactory{}
	service2, err := NewBlobMonitorService(config, mockProducer, realFactory)
	assert.Error(t, err) // Expected to fail without real Azure config
	assert.Nil(t, service2)

	// The key point: same constructor works with different implementations
	// No special testing constructor needed
}

// Validates blob tracking functionality creates and updates trackers correctly
// (No longer applicable: replaced by event-driven processor)
//
// Validates stateless blob closing detection emits closed events for timed-out blobs
func TestStatelessBlobClosing(t *testing.T) {
	config := createTestConfigWithBlobClosing(true, 5)
	mockProducer := NewMockProducer()
	processor := NewBlobClosingProcessor(config, mockProducer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go processor.Start(ctx)

	// Simulate an old blob observed event (should be closed immediately)
	oldTime := time.Now().Add(-10 * time.Minute)
	oldEvent := events.BlobObservedEvent{
		Subscription:     "test-sub",
		Environment:      "test-env",
		BlobName:         "old-blob.log",
		ObservationDate:  oldTime,
		SizeInBytes:      2048,
		LastModifiedDate: oldTime,
		ServiceSelector:  "apache-proxy",
	}
	processor.ProcessBlobObserved(oldEvent)

	// Simulate a new blob observed event (should NOT be closed)
	newTime := time.Now()
	newEvent := events.BlobObservedEvent{
		Subscription:     "test-sub",
		Environment:      "test-env",
		BlobName:         "new-blob.log",
		ObservationDate:  newTime,
		SizeInBytes:      1024,
		LastModifiedDate: newTime,
		ServiceSelector:  "apache-proxy",
	}
	processor.ProcessBlobObserved(newEvent)

	// Give goroutines a moment to publish events
	time.Sleep(20 * time.Millisecond)

	// Verify BlobClosed event was published for the old blob only
	messages := mockProducer.GetProducedMessages()
	require.NotEmpty(t, messages, "Should have at least one BlobClosed event")

	var foundOldClosed, foundNewClosed bool
	for _, msg := range messages {
		if string(msg.Key) == "test-sub:test-env:closed:old-blob.log" {
			foundOldClosed = true
			assert.Contains(t, string(msg.Value), "old-blob.log")
			assert.Contains(t, string(msg.Value), "test-sub")
		}
		if string(msg.Key) == "test-sub:test-env:closed:new-blob.log" {
			foundNewClosed = true
		}
	}
	assert.True(t, foundOldClosed, "Old blob should be closed and event published")
	assert.False(t, foundNewClosed, "New blob should NOT be closed")

	// Test stateless behavior: same old blob will emit closed event again
	// (deduplication handled downstream)
	processor.ProcessBlobObserved(oldEvent)
	time.Sleep(10 * time.Millisecond)

	// Should generate another closed event (stateless - no tracking)
	finalMessages := mockProducer.GetProducedMessages()
	assert.Greater(t, len(finalMessages), len(messages), "Should generate duplicate closed event (stateless)")
}

// Validates blob closing is skipped when disabled in configuration
func TestBlobClosingDisabled(t *testing.T) {
	config := createTestConfigWithBlobClosing(false, 5)
	mockProducer := NewMockProducer()

	// When blob closing is disabled, consumer should be nil
	processor := NewBlobClosingProcessor(config, mockProducer)

	// Simulate a blob observed event
	event := events.BlobObservedEvent{
		Subscription:     "test-sub",
		Environment:      "test-env",
		BlobName:         "test-blob.log",
		ObservationDate:  time.Now(),
		SizeInBytes:      1024,
		LastModifiedDate: time.Now(),
		ServiceSelector:  "apache-proxy",
	}
	processor.ProcessBlobObserved(event)

	// Give it a moment to process
	time.Sleep(10 * time.Millisecond)

	// No closed events should be published (since blob closing is disabled)
	messages := mockProducer.GetProducedMessages()
	for _, msg := range messages {
		assert.NotEqual(t, "test-sub:test-env:closed:test-blob.log", string(msg.Key))
	}
}

// Validates blob tracker updates correctly when blob is modified
// (No longer applicable: replaced by event-driven processor)
//

// (No longer applicable: state reconstruction removed with simplified design)

func createTestConfig() *blobConfig.Config {
	return createTestConfigWithBlobClosing(false, 5)
}

func createTestConfigWithBlobClosing(enabled bool, timeoutMinutes int) *blobConfig.Config {
	daysBack := 1
	return &blobConfig.Config{
		Kafka: blobConfig.KafkaConfig{
			Brokers: "localhost:9092",
			Topic:   "test-topic",
		},
		Global: blobConfig.GlobalConfig{
			PollingInterval: 300,
			BlobClosingConfig: blobConfig.BlobClosingConfig{
				Enabled:        enabled,
				TimeoutMinutes: timeoutMinutes,
			},
		},
		DateRange: blobConfig.DateRangeConfig{
			DaysBack:          &daysBack,
			MonitorCurrentDay: false,
		},
		Environments: []blobConfig.EnvironmentConfig{
			{
				Subscription: "test-sub",
				Environment:  "test-env",
				Enabled:      true,
				Selectors:    []string{"apache-proxy"},
			},
		},
		Storage: blobConfig.StorageConfig{
			ContainerName: "test-container",
		},
		Monitoring: blobConfig.MonitoringConfig{
			LogLevel: "info",
		},
		ErrorHandling: blobConfig.ErrorHandlingConfig{
			ContinueOnEnvironmentError: true,
		},
	}
}
