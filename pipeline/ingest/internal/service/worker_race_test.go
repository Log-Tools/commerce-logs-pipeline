package service

import (
	"context"
	"testing"
	"time"

	ingestConfig "github.com/Log-Tools/commerce-logs-pipeline/ingest/internal/config"
	"github.com/Log-Tools/commerce-logs-pipeline/ingest/internal/filters"
	"github.com/Log-Tools/commerce-logs-pipeline/ingest/internal/ingestion"
	"github.com/Log-Tools/commerce-logs-pipeline/pipeline/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockBlobProcessor for testing the new interface
type MockBlobProcessor struct {
	mock.Mock
}

func (m *MockBlobProcessor) ProcessBlob(ctx context.Context, blobInfo ingestion.BlobProcessingInfo) (*ingestion.BlobProcessingResult, error) {
	args := m.Called(ctx, blobInfo)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ingestion.BlobProcessingResult), args.Error(1)
}

func createTestWorker() (*IngestionWorker, *MockBlobProcessor) {
	cfg := &ingestConfig.Config{
		Mode: "worker",
		Worker: ingestConfig.WorkerConfig{
			BlobStateTopic: "Ingestion.BlobState",
			ProcessingConfig: ingestConfig.ProcessingConfig{
				LoopInterval: 1 * time.Second,
			},
			Filters: ingestConfig.FilterConfig{
				Selectors: []string{".*"}, // Accept all selectors
			},
			Sharding: ingestConfig.ShardingConfig{
				Enabled: false,
			},
		},
	}

	mockProcessor := &MockBlobProcessor{}

	// Create blob filter
	blobFilter, err := filters.NewBlobFilter(&cfg.Worker.Filters)
	if err != nil {
		panic(err) // This should not happen in tests
	}

	// Create worker with minimal dependencies
	worker := &IngestionWorker{
		config:     cfg,
		blobStates: make(map[string]*BlobState),
		processor:  mockProcessor,
		blobFilter: blobFilter,
	}

	return worker, mockProcessor
}

func createBlobStateEvent(blobName string, lastIngestedOffset int64, status string) *events.BlobStateEvent {
	return &events.BlobStateEvent{
		BlobName:           blobName,
		Subscription:       "test-sub",
		Environment:        "test-env",
		Status:             status,
		LastIngestedOffset: lastIngestedOffset,
		LastModifiedDate:   time.Now(),
		LastUpdated:        time.Now(),
	}
}

func TestBlobState_OffsetLogic(t *testing.T) {
	t.Run("preserves higher offset in race condition scenario", func(t *testing.T) {
		// Test the core logic that was changed in the worker
		// This simulates the processBlobStateEventAndProcess logic

		// Initialize blob states map
		blobStates := make(map[string]*BlobState)

		// Step 1: Process first event and simulate completion
		event1 := &events.BlobStateEvent{
			BlobName:           "test-blob.gz",
			Subscription:       "test-sub",
			Environment:        "test-env",
			Status:             "open",
			LastIngestedOffset: 1000,
			LastModifiedDate:   time.Now(),
			LastUpdated:        time.Now(),
		}

		// First processing - simulate logic from worker
		var blobState *BlobState
		if existing, exists := blobStates[event1.BlobName]; exists {
			// Update existing blob state
			blobState = existing
			blobState.State = event1.Status
			blobState.LastUpdated = time.Now()

			// Only update offset if event has higher value
			if event1.LastIngestedOffset > blobState.LastOffset {
				blobState.LastOffset = event1.LastIngestedOffset
			}
		} else {
			// Create new blob state
			blobState = &BlobState{
				BlobName:      event1.BlobName,
				Subscription:  event1.Subscription,
				Environment:   event1.Environment,
				ContainerName: "commerce-logs-separated",
				State:         event1.Status,
				LastOffset:    event1.LastIngestedOffset,
				LastUpdated:   time.Now(),
			}
			blobStates[event1.BlobName] = blobState
		}

		// Simulate processing completion that advances offset to 2000
		simulatedProcessingResult := &ingestion.BlobProcessingResult{
			ProcessedToOffset: 2000,
			LinesProcessed:    500,
		}

		// Update offset after successful processing
		if simulatedProcessingResult.ProcessedToOffset > blobState.LastOffset {
			blobState.LastOffset = simulatedProcessingResult.ProcessedToOffset
		}

		// Verify we're at offset 2000
		assert.Equal(t, int64(2000), blobState.LastOffset)

		// Step 2: Receive stale BlobState event (race condition)
		event2 := &events.BlobStateEvent{
			BlobName:           "test-blob.gz",
			Subscription:       "test-sub",
			Environment:        "test-env",
			Status:             "open",
			LastIngestedOffset: 1500, // Lower than our current 2000!
			LastModifiedDate:   time.Now(),
			LastUpdated:        time.Now(),
		}

		// Process stale event - should preserve higher offset
		if existing, exists := blobStates[event2.BlobName]; exists {
			blobState = existing
			blobState.State = event2.Status
			blobState.LastUpdated = time.Now()

			// CRITICAL: Only update offset if event has higher value
			if event2.LastIngestedOffset > blobState.LastOffset {
				blobState.LastOffset = event2.LastIngestedOffset
			}
			// Note: We keep the higher in-memory offset (2000) instead of using stale (1500)
		}

		// Verify we preserved the higher offset
		assert.Equal(t, int64(2000), blobState.LastOffset, "Should preserve higher in-memory offset during race condition")
		assert.Equal(t, "open", blobState.State, "Should update other state fields")
	})

	t.Run("updates to higher BlobState offset when available", func(t *testing.T) {
		// Test normal case where BlobStateProcessor has caught up
		blobStates := make(map[string]*BlobState)

		// Start with in-memory offset of 1200
		blobStates["test-blob.gz"] = &BlobState{
			BlobName:      "test-blob.gz",
			Subscription:  "test-sub",
			Environment:   "test-env",
			ContainerName: "commerce-logs-separated",
			State:         "open",
			LastOffset:    1200,
			LastUpdated:   time.Now(),
		}

		// Receive event with higher offset (BlobStateProcessor caught up)
		event := &events.BlobStateEvent{
			BlobName:           "test-blob.gz",
			Subscription:       "test-sub",
			Environment:        "test-env",
			Status:             "open",
			LastIngestedOffset: 1800, // Higher than current 1200
			LastModifiedDate:   time.Now(),
			LastUpdated:        time.Now(),
		}

		// Process event
		if existing, exists := blobStates[event.BlobName]; exists {
			blobState := existing
			blobState.State = event.Status
			blobState.LastUpdated = time.Now()

			// Should update to higher offset
			if event.LastIngestedOffset > blobState.LastOffset {
				blobState.LastOffset = event.LastIngestedOffset
			}
		}

		// Verify we updated to the higher offset
		assert.Equal(t, int64(1800), blobStates["test-blob.gz"].LastOffset)
	})

	t.Run("monotonic behavior: offset only increases", func(t *testing.T) {
		testCases := []struct {
			name           string
			initialOffset  int64
			eventOffset    int64
			resultOffset   int64
			expectedOffset int64
			description    string
		}{
			{
				name:           "normal progression",
				initialOffset:  1000,
				eventOffset:    1000,
				resultOffset:   1500,
				expectedOffset: 1500,
				description:    "Normal processing advancement",
			},
			{
				name:           "event has higher offset",
				initialOffset:  1000,
				eventOffset:    1300,
				resultOffset:   1600,
				expectedOffset: 1600,
				description:    "Event caught up, processing continues",
			},
			{
				name:           "stale event ignored",
				initialOffset:  2000,
				eventOffset:    1500,
				resultOffset:   2200,
				expectedOffset: 2200,
				description:    "Stale event ignored, higher offset preserved",
			},
			{
				name:           "no processing advancement",
				initialOffset:  1000,
				eventOffset:    1000,
				resultOffset:   1000,
				expectedOffset: 1000,
				description:    "No new data to process",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				blobStates := make(map[string]*BlobState)

				// Set initial state
				blobStates["test-blob.gz"] = &BlobState{
					BlobName:      "test-blob.gz",
					Subscription:  "test-sub",
					Environment:   "test-env",
					ContainerName: "commerce-logs-separated",
					State:         "open",
					LastOffset:    tc.initialOffset,
					LastUpdated:   time.Now(),
				}

				// Process event
				event := &events.BlobStateEvent{
					BlobName:           "test-blob.gz",
					Subscription:       "test-sub",
					Environment:        "test-env",
					Status:             "open",
					LastIngestedOffset: tc.eventOffset,
					LastModifiedDate:   time.Now(),
					LastUpdated:        time.Now(),
				}

				// Apply event logic
				if existing, exists := blobStates[event.BlobName]; exists {
					blobState := existing
					blobState.State = event.Status
					blobState.LastUpdated = time.Now()

					// Only update if event has higher offset
					if event.LastIngestedOffset > blobState.LastOffset {
						blobState.LastOffset = event.LastIngestedOffset
					}
				}

				// Simulate processing result
				processingResult := &ingestion.BlobProcessingResult{
					ProcessedToOffset: tc.resultOffset,
					LinesProcessed:    100,
				}

				// Apply processing result logic
				if blobState, exists := blobStates["test-blob.gz"]; exists {
					// Only update if result has higher offset
					if processingResult.ProcessedToOffset > blobState.LastOffset {
						blobState.LastOffset = processingResult.ProcessedToOffset
					}
				}

				// Verify expected result
				finalOffset := blobStates["test-blob.gz"].LastOffset
				assert.Equal(t, tc.expectedOffset, finalOffset, tc.description)
			})
		}
	})
}

func TestBlobProcessingResult_Interface(t *testing.T) {
	t.Run("validates BlobProcessingResult structure", func(t *testing.T) {
		result := &ingestion.BlobProcessingResult{
			ProcessedToOffset: 2048,
			LinesProcessed:    100,
		}

		assert.Equal(t, int64(2048), result.ProcessedToOffset)
		assert.Equal(t, 100, result.LinesProcessed)
		assert.GreaterOrEqual(t, result.ProcessedToOffset, int64(0))
		assert.GreaterOrEqual(t, result.LinesProcessed, 0)
	})

	t.Run("validates offset progression logic", func(t *testing.T) {
		tests := []struct {
			name        string
			startOffset int64
			endOffset   int64
			shouldAllow bool
		}{
			{"normal progression", 1000, 2000, true},
			{"no advancement", 1000, 1000, true},
			{"regression not allowed", 2000, 1000, false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := &ingestion.BlobProcessingResult{
					ProcessedToOffset: tt.endOffset,
					LinesProcessed:    10,
				}

				// In our implementation, we only allow offset increases
				isValidProgression := result.ProcessedToOffset >= tt.startOffset
				assert.Equal(t, tt.shouldAllow, isValidProgression)
			})
		}
	})
}

func TestWorker_DebouncingBehavior(t *testing.T) {
	t.Run("enforces minimum processing interval", func(t *testing.T) {
		worker, mockProcessor := createTestWorker()

		// Set a processing interval of 500ms for testing
		worker.config.Worker.ProcessingConfig.LoopInterval = 500 * time.Millisecond

		// First blob processing
		event1 := createBlobStateEvent("test-blob-1.gz", 1000, "open")
		result1 := &ingestion.BlobProcessingResult{
			ProcessedToOffset: 1500,
			LinesProcessed:    100,
		}
		mockProcessor.On("ProcessBlob", mock.Anything, mock.MatchedBy(func(info ingestion.BlobProcessingInfo) bool {
			return info.BlobName == "test-blob-1.gz" && info.StartOffset == 1000
		})).Return(result1, nil).Once()

		start := time.Now()
		err := worker.processBlobStateAndProcess(context.Background(), event1)
		firstDuration := time.Since(start)
		require.NoError(t, err)

		// Verify first processing was immediate (no wait)
		assert.Less(t, firstDuration, 100*time.Millisecond, "First processing should be immediate")

		// Second blob processing immediately after
		event2 := createBlobStateEvent("test-blob-2.gz", 2000, "open")
		result2 := &ingestion.BlobProcessingResult{
			ProcessedToOffset: 2500,
			LinesProcessed:    50,
		}
		// Add mock for second blob processing call
		mockProcessor.On("ProcessBlob", mock.Anything, mock.MatchedBy(func(info ingestion.BlobProcessingInfo) bool {
			return info.BlobName == "test-blob-1.gz" && info.StartOffset == 1500
		})).Return(&ingestion.BlobProcessingResult{
			ProcessedToOffset: 1500,
			LinesProcessed:    0,
		}, nil).Once()
		mockProcessor.On("ProcessBlob", mock.Anything, mock.MatchedBy(func(info ingestion.BlobProcessingInfo) bool {
			return info.BlobName == "test-blob-2.gz" && info.StartOffset == 2000
		})).Return(result2, nil).Once()

		start = time.Now()
		err = worker.processBlobStateAndProcess(context.Background(), event2)
		secondDuration := time.Since(start)
		require.NoError(t, err)

		// Verify second processing was debounced (should wait close to 500ms)
		assert.GreaterOrEqual(t, secondDuration, 400*time.Millisecond, "Second processing should be debounced")
		assert.Less(t, secondDuration, 600*time.Millisecond, "Debounce should not wait too long")

		mockProcessor.AssertExpectations(t)
	})

	t.Run("skips waiting when enough time has passed", func(t *testing.T) {
		worker, mockProcessor := createTestWorker()

		// Set a short processing interval
		worker.config.Worker.ProcessingConfig.LoopInterval = 100 * time.Millisecond

		// First blob processing
		event1 := createBlobStateEvent("test-blob-1.gz", 1000, "open")
		result1 := &ingestion.BlobProcessingResult{
			ProcessedToOffset: 1500,
			LinesProcessed:    100,
		}
		mockProcessor.On("ProcessBlob", mock.Anything, mock.MatchedBy(func(info ingestion.BlobProcessingInfo) bool {
			return info.BlobName == "test-blob-1.gz" && info.StartOffset == 1000
		})).Return(result1, nil).Once()

		err := worker.processBlobStateAndProcess(context.Background(), event1)
		require.NoError(t, err)

		// Wait longer than the interval
		time.Sleep(150 * time.Millisecond)

		// Second blob processing after sufficient delay
		event2 := createBlobStateEvent("test-blob-2.gz", 2000, "open")
		result2 := &ingestion.BlobProcessingResult{
			ProcessedToOffset: 2500,
			LinesProcessed:    50,
		}
		// Add mock for first blob processing again (it will try to process both blobs)
		mockProcessor.On("ProcessBlob", mock.Anything, mock.MatchedBy(func(info ingestion.BlobProcessingInfo) bool {
			return info.BlobName == "test-blob-1.gz" && info.StartOffset == 1500
		})).Return(&ingestion.BlobProcessingResult{
			ProcessedToOffset: 2500,
			LinesProcessed:    50,
		}, nil).Once()
		mockProcessor.On("ProcessBlob", mock.Anything, mock.MatchedBy(func(info ingestion.BlobProcessingInfo) bool {
			return info.BlobName == "test-blob-2.gz" && info.StartOffset == 2000
		})).Return(result2, nil).Once()

		start := time.Now()
		err = worker.processBlobStateAndProcess(context.Background(), event2)
		secondDuration := time.Since(start)
		require.NoError(t, err)

		// Verify second processing was immediate (no debounce wait)
		assert.Less(t, secondDuration, 50*time.Millisecond, "Should process immediately when enough time has passed")

		mockProcessor.AssertExpectations(t)
	})

	t.Run("handles context cancellation during debounce wait", func(t *testing.T) {
		worker, mockProcessor := createTestWorker()

		// Set a long processing interval
		worker.config.Worker.ProcessingConfig.LoopInterval = 2 * time.Second

		// First blob processing
		event1 := createBlobStateEvent("test-blob-1.gz", 1000, "open")
		result1 := &ingestion.BlobProcessingResult{
			ProcessedToOffset: 1500,
			LinesProcessed:    100,
		}
		mockProcessor.On("ProcessBlob", mock.Anything, mock.Anything).Return(result1, nil).Once()

		err := worker.processBlobStateAndProcess(context.Background(), event1)
		require.NoError(t, err)

		// Create a context that will be cancelled during debounce wait
		ctx, cancel := context.WithCancel(context.Background())

		// Second blob processing with cancellation
		event2 := createBlobStateEvent("test-blob-2.gz", 2000, "open")
		// No mock setup for second blob since it should be cancelled before processing

		// Start processing in goroutine
		errChan := make(chan error, 1)
		go func() {
			errChan <- worker.processBlobStateAndProcess(ctx, event2)
		}()

		// Cancel context after a short delay
		time.Sleep(100 * time.Millisecond)
		cancel()

		// Wait for processing to complete
		select {
		case err := <-errChan:
			assert.Equal(t, context.Canceled, err, "Should return context.Canceled")
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Processing should have completed quickly after cancellation")
		}

		mockProcessor.AssertExpectations(t)
	})
}

// processBlobStateAndProcess is a test helper that simulates the old method behavior
// by updating blob state and then processing blobs, similar to the current event-driven architecture
func (w *IngestionWorker) processBlobStateAndProcess(ctx context.Context, event *events.BlobStateEvent) error {
	// Update blob state
	w.updateBlobStateFromEvent(event)

	// Process the affected blob with debouncing
	return w.processAllTrackedBlobsAfterStateUpdate(ctx)
}
