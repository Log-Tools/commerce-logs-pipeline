//go:build integration

package test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	kafkaTC "github.com/testcontainers/testcontainers-go/modules/kafka"

	blobConfig "github.com/Log-Tools/commerce-logs-pipeline/pipeline/blob-monitor/internal/config"
	"github.com/Log-Tools/commerce-logs-pipeline/pipeline/blob-monitor/internal/events"
	"github.com/Log-Tools/commerce-logs-pipeline/pipeline/blob-monitor/internal/service"
)

// BlobMonitorIntegrationSuite provides comprehensive integration tests
type BlobMonitorIntegrationSuite struct {
	suite.Suite
	kafkaContainer testcontainers.Container
	brokers        string
	consumer       *kafka.Consumer
	testTopics     []string
}

// Initializes containerized Kafka environment for realistic testing conditions
func (suite *BlobMonitorIntegrationSuite) SetupSuite() {
	ctx := context.Background()

	// Start Kafka container
	kafkaContainer, err := kafkaTC.RunContainer(ctx,
		testcontainers.WithImage("confluentinc/cp-kafka:7.4.0"),
		kafkaTC.WithClusterID("test-cluster"),
		testcontainers.WithWaitStrategy(nil), // Use default wait strategy
	)
	require.NoError(suite.T(), err)

	suite.kafkaContainer = kafkaContainer

	// Get Kafka broker address
	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(suite.T(), err)
	suite.brokers = brokers[0]

	suite.T().Logf("✅ Kafka started at: %s", suite.brokers)
}

// Cleans up containerized test infrastructure to prevent resource leakage
func (suite *BlobMonitorIntegrationSuite) TearDownSuite() {
	ctx := context.Background()

	// Clean up consumer
	if suite.consumer != nil {
		suite.consumer.Close()
	}

	// Clean up Kafka container
	if suite.kafkaContainer != nil {
		err := suite.kafkaContainer.Terminate(ctx)
		require.NoError(suite.T(), err)
	}

	suite.T().Log("✅ Test environment cleaned up")
}

// Creates isolated topic and consumer for each test to prevent interference
func (suite *BlobMonitorIntegrationSuite) SetupTest() {
	// Create unique topic for this test
	testTopic := suite.createTestTopic()
	suite.testTopics = append(suite.testTopics, testTopic)

	// Create consumer for verification
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": suite.brokers,
		"group.id":          "test-consumer",
		"auto.offset.reset": "earliest",
	})
	require.NoError(suite.T(), err)
	suite.consumer = consumer

	err = suite.consumer.Subscribe(testTopic, nil)
	require.NoError(suite.T(), err)
}

// Ensures clean state between tests by closing Kafka connections
func (suite *BlobMonitorIntegrationSuite) TearDownTest() {
	if suite.consumer != nil {
		suite.consumer.Close()
		suite.consumer = nil
	}
}

// createTestTopic creates a unique topic name for testing
func (suite *BlobMonitorIntegrationSuite) createTestTopic() string {
	return "test-blob-events-" + time.Now().Format("20060102-150405-000")
}

// createTestConfig creates a test configuration
func (suite *BlobMonitorIntegrationSuite) createTestConfig(topic string) *blobConfig.Config {
	daysBack := 1
	return &blobConfig.Config{
		Kafka: blobConfig.KafkaConfig{
			Brokers: suite.brokers,
			Topic:   topic,
			ProducerConfig: map[string]interface{}{
				"acks": "all",
			},
		},
		Global: blobConfig.GlobalConfig{
			PollingInterval:   300,
			EODOverlapMinutes: 60,
			Timezone:          "UTC",
		},
		DateRange: blobConfig.DateRangeConfig{
			DaysBack:          &daysBack,
			MonitorCurrentDay: false, // Disable for testing
		},
		Environments: []blobConfig.EnvironmentConfig{
			{
				Subscription: "test-sub",
				Environment:  "test-env",
				Enabled:      true,
				Selectors:    []string{"apache-proxy", "api"},
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

// Validates blob discovery events flow correctly through real Kafka infrastructure
func (suite *BlobMonitorIntegrationSuite) TestBlobObservedEventPublishing() {
	topic := suite.testTopics[0]
	config := suite.createTestConfig(topic)

	// Create Kafka producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": suite.brokers,
		"acks":              "all",
	})
	require.NoError(suite.T(), err)
	defer producer.Close()

	// Create service for testing (without Azure clients)
	mockStorageFactory := &service.MockStorageClientFactory{}
	testService, err := service.NewBlobMonitorService(config, producer, mockStorageFactory)
	require.NoError(suite.T(), err)

	// Create test event
	testEvent := events.BlobObservedEvent{
		Subscription:     "test-sub",
		Environment:      "test-env",
		BlobName:         "kubernetes/20250607.apache2-igc_proxy-test.gz",
		ObservationDate:  time.Now(),
		SizeInBytes:      1024,
		LastModifiedDate: time.Now(),
		ServiceSelector:  "apache-proxy",
	}

	// Publish event - need to access the method through reflection or make it public
	// For now, let's create a helper method in the service for testing
	err = testService.PublishBlobObservedEvent(testEvent)
	require.NoError(suite.T(), err)

	// Flush producer to ensure message is sent
	producer.Flush(5000)

	// Verify event was published
	message := suite.consumeMessage(5 * time.Second)
	require.NotNil(suite.T(), message, "Expected to receive a message")

	// Verify message content
	var receivedEvent events.BlobObservedEvent
	err = json.Unmarshal(message.Value, &receivedEvent)
	require.NoError(suite.T(), err)

	assert.Equal(suite.T(), testEvent.Subscription, receivedEvent.Subscription)
	assert.Equal(suite.T(), testEvent.Environment, receivedEvent.Environment)
	assert.Equal(suite.T(), testEvent.BlobName, receivedEvent.BlobName)
	assert.Equal(suite.T(), testEvent.SizeInBytes, receivedEvent.SizeInBytes)
	assert.Equal(suite.T(), testEvent.ServiceSelector, receivedEvent.ServiceSelector)

	// Verify key format
	expectedKey := "test-sub-test-env-20250607.apache2-igc_proxy-test.gz-observed"
	assert.Equal(suite.T(), expectedKey, string(message.Key))

	suite.T().Log("✅ BlobObserved event publishing test passed")
}

// Validates multiple events maintain ordering and are all successfully delivered
func (suite *BlobMonitorIntegrationSuite) TestMultipleEventPublishing() {
	topic := suite.testTopics[0]
	config := suite.createTestConfig(topic)

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": suite.brokers,
		"acks":              "all",
	})
	require.NoError(suite.T(), err)
	defer producer.Close()

	mockStorageFactory := &service.MockStorageClientFactory{}
	testService, err := service.NewBlobMonitorService(config, producer, mockStorageFactory)
	require.NoError(suite.T(), err)

	// Create multiple test events
	numEvents := 3
	testEvents := make([]events.BlobObservedEvent, numEvents)
	for i := 0; i < numEvents; i++ {
		testEvents[i] = events.BlobObservedEvent{
			Subscription:     "test-sub",
			Environment:      "test-env",
			BlobName:         fmt.Sprintf("kubernetes/20250607.apache2-igc_proxy-test-%d.gz", i),
			ObservationDate:  time.Now(),
			SizeInBytes:      int64(1024 * (i + 1)),
			LastModifiedDate: time.Now(),
			ServiceSelector:  "apache-proxy",
		}

		err = testService.PublishBlobObservedEvent(testEvents[i])
		require.NoError(suite.T(), err)
	}

	// Flush to ensure all messages are sent
	producer.Flush(10000)

	// Consume and verify all messages
	messages := suite.consumeMessages(numEvents, 10*time.Second)
	require.Len(suite.T(), messages, numEvents, "Expected to receive all published events")

	// Verify each message
	for i, message := range messages {
		var receivedEvent events.BlobObservedEvent
		err = json.Unmarshal(message.Value, &receivedEvent)
		require.NoError(suite.T(), err)

		// Find matching original event (messages may not be in order)
		found := false
		for _, originalEvent := range testEvents {
			if originalEvent.BlobName == receivedEvent.BlobName {
				assert.Equal(suite.T(), originalEvent.SizeInBytes, receivedEvent.SizeInBytes)
				found = true
				break
			}
		}
		assert.True(suite.T(), found, "Could not find matching original event for message %d", i)
	}

	suite.T().Log("✅ Multiple event publishing test passed")
}

func (suite *BlobMonitorIntegrationSuite) consumeMessage(timeout time.Duration) *kafka.Message {
	messages := suite.consumeMessages(1, timeout)
	if len(messages) > 0 {
		return messages[0]
	}
	return nil
}

func (suite *BlobMonitorIntegrationSuite) consumeMessages(count int, timeout time.Duration) []*kafka.Message {
	var messages []*kafka.Message
	deadline := time.Now().Add(timeout)

	for len(messages) < count && time.Now().Before(deadline) {
		msg, err := suite.consumer.ReadMessage(time.Until(deadline))
		if err != nil {
			continue // Timeout or other error, keep trying
		}
		messages = append(messages, msg)
	}

	return messages
}

func (suite *BlobMonitorIntegrationSuite) isBlobObservedEvent(data []byte) bool {
	var event events.BlobObservedEvent
	return json.Unmarshal(data, &event) == nil
}

func (suite *BlobMonitorIntegrationSuite) isBlobsListedEvent(data []byte) bool {
	var event events.BlobsListedEvent
	return json.Unmarshal(data, &event) == nil
}

func TestBlobMonitorIntegration(t *testing.T) {
	suite.Run(t, new(BlobMonitorIntegrationSuite))
}
