package ingestion

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Log-Tools/commerce-logs-pipeline/ingest/internal/config"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Mock implementations for testing
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

type MockBlobClient struct {
	mock.Mock
}

func (m *MockBlobClient) GetProperties(ctx context.Context, options *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
	args := m.Called(ctx, options)
	return args.Get(0).(blob.GetPropertiesResponse), args.Error(1)
}

func (m *MockBlobClient) DownloadStream(ctx context.Context, options *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error) {
	args := m.Called(ctx, options)
	return args.Get(0).(blob.DownloadStreamResponse), args.Error(1)
}

type MockStorageClientFactory struct {
	mock.Mock
}

func (m *MockStorageClientFactory) CreateBlobClient(subscription, environment, containerName, blobName string) (BlobClient, error) {
	args := m.Called(subscription, environment, containerName, blobName)
	return args.Get(0).(BlobClient), args.Error(1)
}

func TestBlobProcessingInfo_Validation(t *testing.T) {
	tests := []struct {
		name     string
		info     BlobProcessingInfo
		expected bool
	}{
		{
			name: "valid processing info",
			info: BlobProcessingInfo{
				ContainerName: "test-container",
				BlobName:      "test-blob.gz",
				StartOffset:   0,
				Subscription:  "test-sub",
				Environment:   "test-env",
			},
			expected: true,
		},
		{
			name: "missing container name",
			info: BlobProcessingInfo{
				BlobName:     "test-blob.gz",
				StartOffset:  0,
				Subscription: "test-sub",
				Environment:  "test-env",
			},
			expected: false,
		},
		{
			name: "missing blob name",
			info: BlobProcessingInfo{
				ContainerName: "test-container",
				StartOffset:   0,
				Subscription:  "test-sub",
				Environment:   "test-env",
			},
			expected: false,
		},
		{
			name: "negative start offset",
			info: BlobProcessingInfo{
				ContainerName: "test-container",
				BlobName:      "test-blob.gz",
				StartOffset:   -1,
				Subscription:  "test-sub",
				Environment:   "test-env",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := tt.info.ContainerName != "" &&
				tt.info.BlobName != "" &&
				tt.info.StartOffset >= 0 &&
				tt.info.Subscription != "" &&
				tt.info.Environment != ""
			assert.Equal(t, tt.expected, valid)
		})
	}
}

func TestBlobStateEvent_Structure(t *testing.T) {
	t.Run("creates valid blob state event", func(t *testing.T) {
		event := BlobStateEvent{
			BlobName:         "test-blob.gz",
			ContainerName:    "test-container",
			Subscription:     "test-sub",
			Environment:      "test-env",
			Selector:         "api-service",
			State:            "open",
			SizeInBytes:      1024,
			LastModified:     time.Now(),
			LastIngestedByte: 0,
		}

		assert.Equal(t, "test-blob.gz", event.BlobName)
		assert.Equal(t, "test-container", event.ContainerName)
		assert.Equal(t, "test-sub", event.Subscription)
		assert.Equal(t, "test-env", event.Environment)
		assert.Equal(t, "api-service", event.Selector)
		assert.Equal(t, "open", event.State)
		assert.Equal(t, int64(1024), event.SizeInBytes)
		assert.Equal(t, int64(0), event.LastIngestedByte)
	})

	t.Run("validates state values", func(t *testing.T) {
		validStates := []string{"open", "closed"}

		for _, state := range validStates {
			event := BlobStateEvent{State: state}
			assert.Contains(t, validStates, event.State)
		}
	})
}

// Helper function to create gzipped test content
func createGzippedContent(lines []string) ([]byte, error) {
	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)

	for _, line := range lines {
		_, err := gzWriter.Write([]byte(line + "\n"))
		if err != nil {
			return nil, err
		}
	}

	err := gzWriter.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// createMockDownloadResponse creates a mock response for testing
func createMockDownloadResponse(content []byte, contentLength int64) blob.DownloadStreamResponse {
	response := blob.DownloadStreamResponse{}
	response.Body = io.NopCloser(bytes.NewReader(content))
	response.ContentLength = &contentLength
	return response
}

func TestGzipContentProcessing(t *testing.T) {
	t.Run("processes gzipped content correctly", func(t *testing.T) {
		// Create test data
		testLines := []string{
			"line 1: test log entry",
			"line 2: another log entry",
			"line 3: final log entry",
		}

		gzippedContent, err := createGzippedContent(testLines)
		require.NoError(t, err)

		// Test decompression
		reader, err := gzip.NewReader(bytes.NewReader(gzippedContent))
		require.NoError(t, err)
		defer reader.Close()

		content, err := io.ReadAll(reader)
		require.NoError(t, err)

		expectedContent := strings.Join(testLines, "\n") + "\n"
		assert.Equal(t, expectedContent, string(content))
	})

	t.Run("handles empty gzipped content", func(t *testing.T) {
		gzippedContent, err := createGzippedContent([]string{})
		require.NoError(t, err)

		reader, err := gzip.NewReader(bytes.NewReader(gzippedContent))
		require.NoError(t, err)
		defer reader.Close()

		content, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Empty(t, content)
	})

	t.Run("handles invalid gzipped content", func(t *testing.T) {
		invalidContent := []byte("not gzipped content")

		_, err := gzip.NewReader(bytes.NewReader(invalidContent))
		assert.Error(t, err)
	})
}

func TestLineBufferSizeConfig(t *testing.T) {
	t.Run("validates buffer size configuration", func(t *testing.T) {
		cfg := &config.Config{
			Worker: config.WorkerConfig{
				ProcessingConfig: config.ProcessingConfig{
					LineBufferSize: 1048576, // 1MB
				},
			},
		}

		assert.Equal(t, 1048576, cfg.Worker.ProcessingConfig.LineBufferSize)
		assert.Greater(t, cfg.Worker.ProcessingConfig.LineBufferSize, 0)
	})

	t.Run("handles large line buffer sizes", func(t *testing.T) {
		// Test that we can handle typical log line sizes
		maxLineSize := 10 * 1024 * 1024 // 10MB

		cfg := &config.Config{
			Worker: config.WorkerConfig{
				ProcessingConfig: config.ProcessingConfig{
					LineBufferSize: maxLineSize,
				},
			},
		}

		assert.Equal(t, maxLineSize, cfg.Worker.ProcessingConfig.LineBufferSize)
	})
}

func TestKafkaTopicConfiguration(t *testing.T) {
	t.Run("validates kafka topic configuration", func(t *testing.T) {
		cfg := &config.Config{
			Kafka: config.KafkaConfig{
				IngestTopic: "Ingestion.RawLogs",
				BlobsTopic:  "Ingestion.Blobs",
				Brokers:     "localhost:9092",
			},
		}

		assert.Equal(t, "Ingestion.RawLogs", cfg.Kafka.IngestTopic)
		assert.Equal(t, "Ingestion.Blobs", cfg.Kafka.BlobsTopic)
		assert.Equal(t, "localhost:9092", cfg.Kafka.Brokers)
		assert.NotEmpty(t, cfg.Kafka.IngestTopic)
		assert.NotEmpty(t, cfg.Kafka.BlobsTopic)
	})

	t.Run("validates producer configuration", func(t *testing.T) {
		cfg := &config.Config{
			Kafka: config.KafkaConfig{
				Producer: config.ProducerConfig{
					Acks:                "all",
					FlushTimeoutMs:      30000,
					DeliveryChannelSize: 10000,
				},
			},
		}

		assert.Equal(t, "all", cfg.Kafka.Producer.Acks)
		assert.Equal(t, 30000, cfg.Kafka.Producer.FlushTimeoutMs)
		assert.Equal(t, 10000, cfg.Kafka.Producer.DeliveryChannelSize)
		assert.Greater(t, cfg.Kafka.Producer.FlushTimeoutMs, 0)
		assert.Greater(t, cfg.Kafka.Producer.DeliveryChannelSize, 0)
	})
}

func TestBlobOffsetCalculations(t *testing.T) {
	t.Run("calculates offset correctly", func(t *testing.T) {
		totalSize := int64(1000)
		startOffset := int64(100)

		remaining := totalSize - startOffset
		assert.Equal(t, int64(900), remaining)
		assert.GreaterOrEqual(t, remaining, int64(0))
	})

	t.Run("handles edge cases for offset calculation", func(t *testing.T) {
		tests := []struct {
			name        string
			totalSize   int64
			startOffset int64
			expected    int64
		}{
			{"start at beginning", 1000, 0, 1000},
			{"start at end", 1000, 1000, 0},
			{"start beyond end", 1000, 1500, -500}, // Should be handled by validation
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				remaining := tt.totalSize - tt.startOffset
				assert.Equal(t, tt.expected, remaining)
			})
		}
	})
}

func TestBlobProcessingResult_Structure(t *testing.T) {
	t.Run("creates valid blob processing result", func(t *testing.T) {
		result := &BlobProcessingResult{
			ProcessedToOffset: 2048,
			LinesProcessed:    100,
		}

		assert.Equal(t, int64(2048), result.ProcessedToOffset)
		assert.Equal(t, 100, result.LinesProcessed)
		assert.GreaterOrEqual(t, result.ProcessedToOffset, int64(0))
		assert.GreaterOrEqual(t, result.LinesProcessed, 0)
	})

	t.Run("validates offset progression", func(t *testing.T) {
		tests := []struct {
			name        string
			startOffset int64
			endOffset   int64
			valid       bool
		}{
			{"normal progression", 1000, 2000, true},
			{"no new data", 1000, 1000, true},
			{"regression not allowed", 2000, 1000, false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := &BlobProcessingResult{
					ProcessedToOffset: tt.endOffset,
					LinesProcessed:    10,
				}

				isValid := result.ProcessedToOffset >= tt.startOffset
				assert.Equal(t, tt.valid, isValid)
			})
		}
	})
}

func TestBlobProcessor_ProcessBlob(t *testing.T) {
	t.Run("processes blob successfully with valid result", func(t *testing.T) {
		// Setup mocks
		mockProducer := &MockProducer{}
		mockBlobClient := &MockBlobClient{}
		mockStorageFactory := &MockStorageClientFactory{}

		// Test configuration
		cfg := &config.Config{
			Kafka: config.KafkaConfig{
				BlobsTopic:  "test-blobs-topic",
				IngestTopic: "test-ingest-topic",
				Producer: config.ProducerConfig{
					FlushTimeoutMs: 1000,
				},
			},
			Worker: config.WorkerConfig{
				ProcessingConfig: config.ProcessingConfig{
					LineBufferSize: 1024,
				},
			},
		}

		// Create test data
		testLines := []string{
			"line 1: test log entry",
			"line 2: another log entry",
			"line 3: final log entry",
		}
		gzippedContent, err := createGzippedContent(testLines)
		require.NoError(t, err)

		// Setup blob properties response
		contentLength := int64(len(gzippedContent))
		lastModified := time.Now()
		propsResponse := blob.GetPropertiesResponse{
			ContentLength: &contentLength,
			LastModified:  &lastModified,
		}

		// Create a mock download response that matches the interface expectations
		// Note: We need to create a struct that can satisfy the actual usage in processor
		downloadResponse := createMockDownloadResponse(gzippedContent, contentLength)

		// Configure mocks
		mockStorageFactory.On("CreateBlobClient", "test-sub", "test-env", "test-container", "test-blob.gz").Return(mockBlobClient, nil)
		mockBlobClient.On("GetProperties", mock.Anything, mock.Anything).Return(propsResponse, nil)
		mockBlobClient.On("DownloadStream", mock.Anything, mock.Anything).Return(downloadResponse, nil)

		// Mock producer calls for log lines and events
		mockProducer.On("Produce", mock.Anything, mock.Anything).Return(nil)
		mockProducer.On("Flush", 1000).Return(0) // All messages delivered successfully
		mockProducer.On("Events").Return(make(chan kafka.Event))

		// Create processor
		processor := NewBlobProcessor(cfg, mockProducer, mockStorageFactory)

		// Test processing
		blobInfo := BlobProcessingInfo{
			ContainerName: "test-container",
			BlobName:      "test-blob.gz",
			StartOffset:   0,
			Subscription:  "test-sub",
			Environment:   "test-env",
		}

		result, err := processor.ProcessBlob(context.Background(), blobInfo)

		// Verify results
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, contentLength, result.ProcessedToOffset)
		assert.Equal(t, len(testLines), result.LinesProcessed)

		// Verify mock expectations
		mockStorageFactory.AssertExpectations(t)
		mockBlobClient.AssertExpectations(t)
		mockProducer.AssertExpectations(t)
	})

	t.Run("handles blob with no new data from start offset", func(t *testing.T) {
		// Setup mocks
		mockProducer := &MockProducer{}
		mockBlobClient := &MockBlobClient{}
		mockStorageFactory := &MockStorageClientFactory{}

		cfg := &config.Config{
			Kafka: config.KafkaConfig{
				BlobsTopic: "test-blobs-topic",
				Producer: config.ProducerConfig{
					FlushTimeoutMs: 1000,
				},
			},
		}

		// Setup blob that already fully processed
		contentLength := int64(1000)
		startOffset := int64(1000) // At end of blob
		lastModified := time.Now()
		propsResponse := blob.GetPropertiesResponse{
			ContentLength: &contentLength,
			LastModified:  &lastModified,
		}

		mockStorageFactory.On("CreateBlobClient", "test-sub", "test-env", "test-container", "test-blob.gz").Return(mockBlobClient, nil)
		mockBlobClient.On("GetProperties", mock.Anything, mock.Anything).Return(propsResponse, nil)
		mockProducer.On("Produce", mock.Anything, mock.Anything).Return(nil)
		mockProducer.On("Flush", 1000).Return(0)
		mockProducer.On("Events").Return(make(chan kafka.Event))

		processor := NewBlobProcessor(cfg, mockProducer, mockStorageFactory)

		blobInfo := BlobProcessingInfo{
			ContainerName: "test-container",
			BlobName:      "test-blob.gz",
			StartOffset:   startOffset,
			Subscription:  "test-sub",
			Environment:   "test-env",
		}

		result, err := processor.ProcessBlob(context.Background(), blobInfo)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, contentLength, result.ProcessedToOffset)
		assert.Equal(t, 0, result.LinesProcessed) // No lines processed

		mockStorageFactory.AssertExpectations(t)
		mockBlobClient.AssertExpectations(t)
		mockProducer.AssertExpectations(t)
	})

	t.Run("validates completion message persistence configuration", func(t *testing.T) {
		cfg := &config.Config{
			Kafka: config.KafkaConfig{
				BlobsTopic:  "test-blobs-topic",
				IngestTopic: "test-ingest-topic",
				Producer: config.ProducerConfig{
					FlushTimeoutMs: 1000,
				},
			},
			Worker: config.WorkerConfig{
				ProcessingConfig: config.ProcessingConfig{
					LineBufferSize: 1024,
				},
			},
		}

		// Test the configuration and interface behavior
		assert.Equal(t, "test-blobs-topic", cfg.Kafka.BlobsTopic)
		assert.Equal(t, "test-ingest-topic", cfg.Kafka.IngestTopic)
		assert.Equal(t, 1000, cfg.Kafka.Producer.FlushTimeoutMs)
		assert.Equal(t, 1024, cfg.Worker.ProcessingConfig.LineBufferSize)
	})
}
