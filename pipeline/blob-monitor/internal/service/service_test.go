package service

import (
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
	m.messages = append(m.messages, MockMessage{
		Topic: *msg.TopicPartition.Topic,
		Key:   msg.Key,
		Value: msg.Value,
	})

	// Send delivery confirmation
	go func() {
		m.events <- &kafka.Message{
			TopicPartition: msg.TopicPartition,
			Key:            msg.Key,
			Value:          msg.Value,
		}
	}()

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
	return m.messages
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
	// Verify key format follows pattern: {subscription}-{environment}-{cleanBlobName}-observed
	assert.Equal(t, "test-sub-test-env-20250607.apache2-igc_proxy-test.gz-observed", string(msg.Key))
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
	// Verify key format follows pattern: {subscription}-{environment}-{selector}-{date}-listed
	assert.Equal(t, "test-sub-test-env-apache-proxy-20250607-listed", string(msg.Key))
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
	// Verify key format follows pattern: {subscription}-{environment}-{cleanBlobName}-closed
	assert.Equal(t, "test-sub-test-env-20250607.apache2-igc_proxy-test.gz-closed", string(msg.Key))
	// Verify critical fields are present in JSON payload
	assert.Contains(t, string(msg.Value), "test-sub")
	assert.Contains(t, string(msg.Value), "apache-proxy")
	assert.Contains(t, string(msg.Value), "closedDate")
	assert.Contains(t, string(msg.Value), "timeoutMinutes")
}

// Validates Kafka message keys follow consistent format and handle path normalization
func TestGenerateBlobKey(t *testing.T) {
	tests := []struct {
		name         string
		subscription string
		environment  string
		blobName     string
		suffix       string
		expected     string
	}{
		{
			name:         "strips kubernetes prefix for cleaner keys",
			subscription: "cp2",
			environment:  "D1",
			blobName:     "kubernetes/20250607.apache2-igc_proxy-test.gz",
			suffix:       "observed",
			expected:     "cp2-D1-20250607.apache2-igc_proxy-test.gz-observed",
		},
		{
			name:         "handles blobs without kubernetes prefix",
			subscription: "cp2",
			environment:  "S1",
			blobName:     "20250607.api-service-test.gz",
			suffix:       "observed",
			expected:     "cp2-S1-20250607.api-service-test.gz-observed",
		},
		{
			name:         "supports different suffixes for event types",
			subscription: "cp2",
			environment:  "P1",
			blobName:     "kubernetes/20250607.zookeeper-test.gz",
			suffix:       "listed",
			expected:     "cp2-P1-20250607.zookeeper-test.gz-listed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generateBlobKey(tt.subscription, tt.environment, tt.blobName, tt.suffix)
			assert.Equal(t, tt.expected, result)
		})
	}
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
func TestTrackBlob(t *testing.T) {
	config := createTestConfigWithBlobClosing(true, 5)
	mockProducer := NewMockProducer()
	mockStorageFactory := &MockStorageClientFactory{}

	service, err := NewBlobMonitorService(config, mockProducer, mockStorageFactory)
	require.NoError(t, err)

	// Track a new blob
	event := events.BlobObservedEvent{
		Subscription:     "test-sub",
		Environment:      "test-env",
		BlobName:         "test-blob.log",
		ObservationDate:  time.Now(),
		SizeInBytes:      1024,
		LastModifiedDate: time.Now(),
		ServiceSelector:  "apache-proxy",
	}

	service.trackBlob(event)

	// Verify tracker was created
	service.mu.RLock()
	trackers := service.blobTrackers
	service.mu.RUnlock()

	assert.Len(t, trackers, 1)

	blobKey := generateBlobKey(event.Subscription, event.Environment, event.BlobName, "tracker")
	tracker, exists := trackers[blobKey]
	require.True(t, exists)

	assert.Equal(t, event.LastModifiedDate, tracker.LastModified)
	assert.Equal(t, event.SizeInBytes, tracker.SizeInBytes)
	assert.Equal(t, event.ServiceSelector, tracker.ServiceSelector)
	assert.False(t, tracker.IsClosed)
}

// Validates blob closing detection marks inactive blobs as closed
func TestDetectClosedBlobs(t *testing.T) {
	config := createTestConfigWithBlobClosing(true, 5)
	mockProducer := NewMockProducer()
	mockStorageFactory := &MockStorageClientFactory{}

	service, err := NewBlobMonitorService(config, mockProducer, mockStorageFactory)
	require.NoError(t, err)

	// Create a blob tracker that would be considered closed (last modified 10 minutes ago)
	oldTime := time.Now().Add(-10 * time.Minute)
	blobKey := generateBlobKey("test-sub", "test-env", "old-blob.log", "tracker")

	service.mu.Lock()
	service.blobTrackers[blobKey] = &BlobTracker{
		Subscription:    "test-sub",
		Environment:     "test-env",
		BlobName:        "old-blob.log",
		LastModified:    oldTime,
		LastChecked:     time.Now(),
		SizeInBytes:     2048,
		ServiceSelector: "apache-proxy",
		IsClosed:        false,
	}
	service.mu.Unlock()

	// Run blob closing detection
	timeout := 5 * time.Minute
	service.detectClosedBlobs(timeout)

	// Verify blob was marked as closed
	service.mu.RLock()
	tracker := service.blobTrackers[blobKey]
	service.mu.RUnlock()

	assert.True(t, tracker.IsClosed)

	// Verify BlobClosed event was published
	messages := mockProducer.GetProducedMessages()
	require.Len(t, messages, 1)

	msg := messages[0]
	assert.Equal(t, "test-topic", msg.Topic)
	assert.Equal(t, "test-sub-test-env-old-blob.log-closed", string(msg.Key))
	assert.Contains(t, string(msg.Value), "old-blob.log")
	assert.Contains(t, string(msg.Value), "test-sub")
}

// Validates blob closing is skipped when disabled in configuration
func TestBlobClosingDisabled(t *testing.T) {
	config := createTestConfigWithBlobClosing(false, 5)
	mockProducer := NewMockProducer()
	mockStorageFactory := &MockStorageClientFactory{}

	service, err := NewBlobMonitorService(config, mockProducer, mockStorageFactory)
	require.NoError(t, err)

	// Track a blob
	event := events.BlobObservedEvent{
		Subscription:     "test-sub",
		Environment:      "test-env",
		BlobName:         "test-blob.log",
		ObservationDate:  time.Now(),
		SizeInBytes:      1024,
		LastModifiedDate: time.Now(),
		ServiceSelector:  "apache-proxy",
	}

	service.trackBlob(event)

	// Verify no trackers were created (since blob closing is disabled)
	service.mu.RLock()
	trackers := service.blobTrackers
	service.mu.RUnlock()

	assert.Empty(t, trackers)
}

// Validates blob tracker updates correctly when blob is modified
func TestTrackBlobUpdate(t *testing.T) {
	config := createTestConfigWithBlobClosing(true, 5)
	mockProducer := NewMockProducer()
	mockStorageFactory := &MockStorageClientFactory{}

	service, err := NewBlobMonitorService(config, mockProducer, mockStorageFactory)
	require.NoError(t, err)

	// Track initial blob
	initialTime := time.Now().Add(-2 * time.Minute)
	event1 := events.BlobObservedEvent{
		Subscription:     "test-sub",
		Environment:      "test-env",
		BlobName:         "test-blob.log",
		ObservationDate:  initialTime,
		SizeInBytes:      1024,
		LastModifiedDate: initialTime,
		ServiceSelector:  "apache-proxy",
	}
	service.trackBlob(event1)

	// Track updated blob with newer modification time
	updatedTime := time.Now()
	event2 := events.BlobObservedEvent{
		Subscription:     "test-sub",
		Environment:      "test-env",
		BlobName:         "test-blob.log",
		ObservationDate:  updatedTime,
		SizeInBytes:      2048,
		LastModifiedDate: updatedTime,
		ServiceSelector:  "apache-proxy",
	}
	service.trackBlob(event2)

	// Verify tracker was updated with newer information
	service.mu.RLock()
	trackers := service.blobTrackers
	service.mu.RUnlock()

	assert.Len(t, trackers, 1)

	blobKey := generateBlobKey(event1.Subscription, event1.Environment, event1.BlobName, "tracker")
	tracker, exists := trackers[blobKey]
	require.True(t, exists)

	assert.Equal(t, updatedTime, tracker.LastModified)
	assert.Equal(t, int64(2048), tracker.SizeInBytes)
	assert.False(t, tracker.IsClosed)
}

// Validates blob tracker cleanup removes old closed blobs
func TestBlobTrackerCleanup(t *testing.T) {
	config := createTestConfigWithBlobClosing(true, 5)
	mockProducer := NewMockProducer()
	mockStorageFactory := &MockStorageClientFactory{}

	service, err := NewBlobMonitorService(config, mockProducer, mockStorageFactory)
	require.NoError(t, err)

	// Create old closed blob tracker (25 hours ago)
	oldTime := time.Now().Add(-25 * time.Hour)
	oldBlobKey := generateBlobKey("test-sub", "test-env", "old-blob.log", "tracker")

	service.mu.Lock()
	service.blobTrackers[oldBlobKey] = &BlobTracker{
		Subscription:    "test-sub",
		Environment:     "test-env",
		BlobName:        "old-blob.log",
		LastModified:    oldTime,
		LastChecked:     oldTime,
		SizeInBytes:     1024,
		ServiceSelector: "apache-proxy",
		IsClosed:        true,
	}

	// Create recent closed blob tracker (1 hour ago)
	recentTime := time.Now().Add(-1 * time.Hour)
	recentBlobKey := generateBlobKey("test-sub", "test-env", "recent-blob.log", "tracker")
	service.blobTrackers[recentBlobKey] = &BlobTracker{
		Subscription:    "test-sub",
		Environment:     "test-env",
		BlobName:        "recent-blob.log",
		LastModified:    recentTime,
		LastChecked:     recentTime,
		SizeInBytes:     2048,
		ServiceSelector: "apache-proxy",
		IsClosed:        true,
	}
	service.mu.Unlock()

	// Run detection which should clean up old trackers
	timeout := 5 * time.Minute
	service.detectClosedBlobs(timeout)

	// Verify old tracker was cleaned up but recent one remains
	service.mu.RLock()
	trackers := service.blobTrackers
	service.mu.RUnlock()

	_, oldExists := trackers[oldBlobKey]
	_, recentExists := trackers[recentBlobKey]

	assert.False(t, oldExists, "Old closed blob tracker should be cleaned up")
	assert.True(t, recentExists, "Recent closed blob tracker should remain")
}

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
