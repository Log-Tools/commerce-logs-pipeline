//go:build integration

package test

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Log-Tools/commerce-logs-pipeline/ingest/internal/config"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Integration tests require external dependencies:
// - Running Kafka instance
// - Test data setup

func TestKafkaIntegration(t *testing.T) {
	// Skip if Kafka is not available
	if !isKafkaAvailable() {
		t.Skip("Kafka not available for integration tests")
	}

	kafkaBrokers := getKafkaBrokers()
	testTopic := "test-ingest-integration"

	t.Run("can connect to Kafka", func(t *testing.T) {
		// Test producer connection
		producer, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": kafkaBrokers,
			"acks":              "all",
		})
		require.NoError(t, err)
		defer producer.Close()

		// Test consumer connection
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  kafkaBrokers,
			"group.id":           "test-integration-group",
			"auto.offset.reset":  "earliest",
			"enable.auto.commit": false,
		})
		require.NoError(t, err)
		defer consumer.Close()

		// Verify we can get metadata
		metadata, err := producer.GetMetadata(nil, false, 5000)
		require.NoError(t, err)
		assert.NotEmpty(t, metadata.Brokers)
	})

	t.Run("can produce and consume messages", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Create producer
		producer, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": kafkaBrokers,
			"acks":              "all",
		})
		require.NoError(t, err)
		defer producer.Close()

		// Create consumer
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  kafkaBrokers,
			"group.id":           "test-produce-consume-group",
			"auto.offset.reset":  "earliest",
			"enable.auto.commit": false,
		})
		require.NoError(t, err)
		defer consumer.Close()

		// Subscribe to test topic
		err = consumer.SubscribeTopics([]string{testTopic}, nil)
		require.NoError(t, err)

		// Produce test message
		testMessage := "integration test message"
		message := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: kafka.PartitionAny},
			Key:            []byte("test-key"),
			Value:          []byte(testMessage),
		}

		deliveryChan := make(chan kafka.Event, 1)
		err = producer.Produce(message, deliveryChan)
		require.NoError(t, err)

		// Wait for delivery confirmation
		select {
		case e := <-deliveryChan:
			m := e.(*kafka.Message)
			assert.Nil(t, m.TopicPartition.Error)
		case <-ctx.Done():
			t.Fatal("timeout waiting for message delivery")
		}

		// Flush to ensure message is sent
		producer.Flush(5000)

		// Consume message
		for {
			select {
			case <-ctx.Done():
				t.Fatal("timeout waiting for message consumption")
			default:
				ev := consumer.Poll(1000)
				if ev == nil {
					continue
				}

				switch e := ev.(type) {
				case *kafka.Message:
					assert.Equal(t, testMessage, string(e.Value))
					assert.Equal(t, "test-key", string(e.Key))
					return
				case kafka.Error:
					t.Fatalf("kafka error: %v", e)
				}
			}
		}
	})
}

func TestConfigurationIntegration(t *testing.T) {
	t.Run("loads configuration from environment", func(t *testing.T) {
		// Save original env
		originalMode := os.Getenv("INGEST_MODE")
		originalBrokers := os.Getenv("KAFKA_BROKERS")
		defer func() {
			restoreEnv("INGEST_MODE", originalMode)
			restoreEnv("KAFKA_BROKERS", originalBrokers)
		}()

		// Set test environment
		os.Setenv("INGEST_MODE", "cli")
		os.Setenv("KAFKA_BROKERS", "localhost:9092")
		os.Setenv("KAFKA_PROXY_TOPIC", "Raw.ProxyLogs")
		os.Setenv("KAFKA_APP_TOPIC", "Raw.ApplicationLogs")
		os.Setenv("SUBSCRIPTION_ID", "test-sub")
		os.Setenv("ENVIRONMENT", "test-env")
		os.Setenv("AZURE_STORAGE_CONTAINER_NAME", "test-container")
		os.Setenv("AZURE_STORAGE_BLOB_NAME", "test-blob.gz")

		cfg, err := config.LoadConfigFromEnv()
		require.NoError(t, err)

		assert.Equal(t, "cli", cfg.Mode)
		assert.Equal(t, "localhost:9092", cfg.Kafka.Brokers)
		assert.Equal(t, "test-sub", cfg.CLI.SubscriptionID)
		assert.Equal(t, "test-env", cfg.CLI.Environment)
	})

	t.Run("validates loaded configuration", func(t *testing.T) {
		cfg := &config.Config{
			Mode: "cli",
			CLI: config.CLIConfig{
				SubscriptionID: "test-sub",
				Environment:    "test-env",
				ContainerName:  "test-container",
				BlobName:       "test-blob.gz",
				StartOffset:    0,
			},
			Kafka: config.KafkaConfig{
				Brokers:          "localhost:9092",
				ProxyTopic:       "Raw.ProxyLogs",
				ApplicationTopic: "Raw.ApplicationLogs",
				Partitions:       1,
			},
		}

		err := cfg.Validate()
		assert.NoError(t, err)
	})
}

func TestGzipProcessingIntegration(t *testing.T) {
	t.Run("processes real gzipped log data", func(t *testing.T) {
		// Create realistic test log data
		logLines := []string{
			`2024-01-15T10:30:00Z [INFO] Starting application`,
			`2024-01-15T10:30:01Z [INFO] Database connection established`,
			`2024-01-15T10:30:02Z [WARN] High memory usage detected`,
			`2024-01-15T10:30:03Z [ERROR] Failed to process request: timeout`,
			`2024-01-15T10:30:04Z [INFO] Request completed successfully`,
		}

		// Create gzipped content
		var buf bytes.Buffer
		gzWriter := gzip.NewWriter(&buf)
		for _, line := range logLines {
			_, err := gzWriter.Write([]byte(line + "\n"))
			require.NoError(t, err)
		}
		err := gzWriter.Close()
		require.NoError(t, err)

		gzippedData := buf.Bytes()
		assert.Greater(t, len(gzippedData), 0)

		// Test decompression
		reader, err := gzip.NewReader(bytes.NewReader(gzippedData))
		require.NoError(t, err)
		defer reader.Close()

		// Verify we can read all lines
		scanner := bytes.NewBuffer(nil)
		_, err = scanner.ReadFrom(reader)
		require.NoError(t, err)

		content := scanner.String()
		for _, expectedLine := range logLines {
			assert.Contains(t, content, expectedLine)
		}
	})

	t.Run("handles large gzipped files", func(t *testing.T) {
		// Create a larger test file
		var logLines []string
		for i := 0; i < 1000; i++ {
			logLines = append(logLines,
				fmt.Sprintf("2024-01-15T10:30:%02dZ [INFO] Log entry %d with some additional data to make it longer",
					i%60, i))
		}

		// Compress the data
		var buf bytes.Buffer
		gzWriter := gzip.NewWriter(&buf)
		for _, line := range logLines {
			_, err := gzWriter.Write([]byte(line + "\n"))
			require.NoError(t, err)
		}
		err := gzWriter.Close()
		require.NoError(t, err)

		// Calculate original uncompressed size
		originalSize := 0
		for _, line := range logLines {
			originalSize += len(line) + 1 // +1 for newline
		}
		compressedSize := buf.Len()

		// Verify compression worked
		assert.Greater(t, originalSize, compressedSize)
		assert.Greater(t, compressedSize, 0)

		// Verify decompression
		reader, err := gzip.NewReader(&buf)
		require.NoError(t, err)
		defer reader.Close()

		lineCount := 0
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			lineCount++
		}
		require.NoError(t, scanner.Err())
		assert.Equal(t, 1000, lineCount)
	})
}

func TestEndToEndProcessingIntegration(t *testing.T) {
	if !isKafkaAvailable() {
		t.Skip("Kafka not available for integration tests")
	}

	t.Run("simulates blob processing workflow", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		kafkaBrokers := getKafkaBrokers()
		testTopic := "test-e2e-processing"

		// Create test configuration
		cfg := &config.Config{
			Mode: "cli",
			CLI: config.CLIConfig{
				SubscriptionID: "test-sub",
				Environment:    "test-env",
				ContainerName:  "test-container",
				BlobName:       "test-blob.gz",
				StartOffset:    0,
			},
			Kafka: config.KafkaConfig{
				Brokers:          kafkaBrokers,
				IngestTopic:      testTopic,
				BlobsTopic:       testTopic + "-blobs",
				ProxyTopic:       "Raw.ProxyLogs",
				ApplicationTopic: "Raw.ApplicationLogs",
				Partitions:       1,
				Producer: config.ProducerConfig{
					Acks:                "all",
					FlushTimeoutMs:      30000,
					DeliveryChannelSize: 1000,
				},
			},
		}

		err := cfg.Validate()
		require.NoError(t, err)

		// Create Kafka producer for testing
		producer, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": kafkaBrokers,
			"acks":              cfg.Kafka.Producer.Acks,
		})
		require.NoError(t, err)
		defer producer.Close()

		// Create consumer to verify messages
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  kafkaBrokers,
			"group.id":           "test-e2e-consumer",
			"auto.offset.reset":  "earliest",
			"enable.auto.commit": false,
		})
		require.NoError(t, err)
		defer consumer.Close()

		err = consumer.SubscribeTopics([]string{testTopic}, nil)
		require.NoError(t, err)

		// Test message production (simulating what the ingestion would do)
		testMessages := []string{
			"Test log line 1",
			"Test log line 2",
			"Test log line 3",
		}

		for i, msg := range testMessages {
			kafkaMsg := &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: kafka.PartitionAny},
				Key:            []byte(fmt.Sprintf("line-%d", i+1)),
				Value:          []byte(msg),
			}

			deliveryChan := make(chan kafka.Event, 1)
			err := producer.Produce(kafkaMsg, deliveryChan)
			require.NoError(t, err)

			// Wait for delivery
			select {
			case e := <-deliveryChan:
				deliveredMsg := e.(*kafka.Message)
				assert.Nil(t, deliveredMsg.TopicPartition.Error)
			case <-ctx.Done():
				t.Fatal("timeout waiting for message delivery")
			}
		}

		producer.Flush(5000)

		// Verify messages were consumed
		consumedCount := 0
		for consumedCount < len(testMessages) {
			select {
			case <-ctx.Done():
				t.Fatalf("timeout waiting for messages, got %d of %d", consumedCount, len(testMessages))
			default:
				ev := consumer.Poll(1000)
				if ev == nil {
					continue
				}

				switch e := ev.(type) {
				case *kafka.Message:
					expectedMsg := testMessages[consumedCount]
					assert.Equal(t, expectedMsg, string(e.Value))
					consumedCount++
				case kafka.Error:
					t.Fatalf("kafka error: %v", e)
				}
			}
		}

		assert.Equal(t, len(testMessages), consumedCount)
	})
}

// Helper functions

func isKafkaAvailable() bool {
	brokers := getKafkaBrokers()
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return false
	}
	defer producer.Close()

	_, err = producer.GetMetadata(nil, false, 5000)
	return err == nil
}

func getKafkaBrokers() string {
	brokers := os.Getenv("KAFKA_BROKERS_TEST")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	return brokers
}

func restoreEnv(key, value string) {
	if value == "" {
		os.Unsetenv(key)
	} else {
		os.Setenv(key, value)
	}
}
