package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Log-Tools/commerce-logs-pipeline/config"
	ingestConfig "github.com/Log-Tools/commerce-logs-pipeline/ingest/internal/config"
	"github.com/Log-Tools/commerce-logs-pipeline/ingest/internal/filters"
	"github.com/Log-Tools/commerce-logs-pipeline/ingest/internal/ingestion"
	"github.com/Log-Tools/commerce-logs-pipeline/pipeline/events"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// IngestionWorker manages blob processing workflow
type IngestionWorker struct {
	config         *ingestConfig.Config
	storageConfig  *config.Config // Main project storage config
	blobFilter     *filters.BlobFilter
	producer       ingestion.Producer
	consumer       ingestion.Consumer
	storageFactory ingestion.StorageClientFactory
	processor      ingestion.BlobProcessor

	// State tracking (no mutex needed in single-threaded event loop)
	blobStates map[string]*BlobState
	stopChan   chan struct{}

	// Debouncing control
	lastIterationStart time.Time
}

// BlobState tracks processing state for a blob
type BlobState struct {
	BlobName      string
	Subscription  string
	Environment   string
	ContainerName string
	State         string // "open" or "closed"
	LastOffset    int64  // Last processed offset
	LastUpdated   time.Time
}

// NewIngestionWorker creates a new ingestion worker
func NewIngestionWorker(
	cfg *ingestConfig.Config,
	producer ingestion.Producer,
	consumer ingestion.Consumer,
	storageFactory ingestion.StorageClientFactory,
) (*IngestionWorker, error) {
	// Load storage configuration
	storageConfig, err := config.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load storage configuration: %w", err)
	}

	// Create blob filter
	blobFilter, err := filters.NewBlobFilter(&cfg.Worker.Filters)
	if err != nil {
		return nil, fmt.Errorf("failed to create blob filter: %w", err)
	}

	// Create blob processor with correct signature
	processor := ingestion.NewBlobProcessor(cfg, producer, storageFactory)

	return &IngestionWorker{
		config:         cfg,
		storageConfig:  storageConfig,
		blobFilter:     blobFilter,
		producer:       producer,
		consumer:       consumer,
		storageFactory: storageFactory,
		processor:      processor,
		blobStates:     make(map[string]*BlobState),
		stopChan:       make(chan struct{}),
	}, nil
}

// Start begins the ingestion worker processing loop
func (w *IngestionWorker) Start(ctx context.Context) error {
	log.Printf("Starting ingestion worker in %s mode", w.config.Mode)

	// Subscribe to blob state topic
	if err := w.consumer.Subscribe([]string{w.config.Worker.BlobStateTopic}, nil); err != nil {
		return fmt.Errorf("failed to subscribe to blob state topic: %w", err)
	}

	log.Printf("📋 Subscribed to blob state topic: %s", w.config.Worker.BlobStateTopic)

	// Single event-driven loop: consume events and process immediately
	return w.eventDrivenLoop(ctx)
}

// Stop gracefully shuts down the worker
func (w *IngestionWorker) Stop() error {
	log.Println("Stopping ingestion worker...")
	close(w.stopChan)
	return w.consumer.Close()
}

// eventDrivenLoop processes blob state events and immediately processes blobs
func (w *IngestionWorker) eventDrivenLoop(ctx context.Context) error {
	log.Printf("🔄 Starting event-driven loop, polling for messages...")
	pollCount := 0

	for {
		select {
		case <-ctx.Done():
			log.Printf("🛑 Context cancelled, stopping event loop...")
			return ctx.Err()
		case <-w.stopChan:
			log.Printf("🛑 Stop signal received, stopping event loop...")
			return nil
		default:
			// Consume all available messages in a batch
			messages, err := w.consumeAllAvailableMessages(ctx)
			if err != nil {
				log.Printf("Error consuming messages: %v", err)
				continue
			}

			if len(messages) == 0 {
				pollCount++
				// Log every 10 polls to show we're alive but not getting messages
				if pollCount%10 == 0 {
					log.Printf("🔍 Polled %d times, no messages received yet...", pollCount)
				}
				// Add a small delay when no messages to reduce CPU usage
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Reset poll count when we receive messages
			pollCount = 0

			log.Printf("📨 Received batch of %d message(s)", len(messages))

			// Update state for all consumed messages
			if err := w.updateStateFromMessages(ctx, messages); err != nil {
				log.Printf("Error updating state from messages: %v", err)
				continue
			}

			// Process all affected blobs after state update
			if err := w.processAllTrackedBlobsAfterStateUpdate(ctx); err != nil {
				log.Printf("Error processing blobs after state update: %v", err)
			}
		}
	}
}

// consumeAllAvailableMessages drains all currently available messages from the consumer
func (w *IngestionWorker) consumeAllAvailableMessages(ctx context.Context) ([]*kafka.Message, error) {
	var messages []*kafka.Message

	// Use a shorter timeout for the first poll to be more responsive
	event := w.consumer.Poll(100) // 100ms timeout instead of 1000ms

	if event == nil {
		return messages, nil // No messages available
	}

	// Process the first event
	if msg, ok := event.(*kafka.Message); ok {
		messages = append(messages, msg)
		log.Printf("📨 First message: offset %d", msg.TopicPartition.Offset)
	} else {
		// Handle non-message events
		w.handleNonMessageEvent(event)
		return messages, nil
	}

	// Now quickly drain any additional messages
	maxDrainAttempts := 1000 // Prevent infinite loops
	for i := 0; i < maxDrainAttempts; i++ {
		select {
		case <-ctx.Done():
			return messages, ctx.Err()
		default:
			event := w.consumer.Poll(0) // Zero timeout for immediate return
			if event == nil {
				break // No more messages available
			}

			if msg, ok := event.(*kafka.Message); ok {
				messages = append(messages, msg)
				if len(messages)%10 == 0 {
					log.Printf("📨 Collected %d messages so far...", len(messages))
				}
			} else {
				// Handle non-message events
				w.handleNonMessageEvent(event)
			}
		}
	}

	log.Printf("📊 Total collected: %d messages", len(messages))
	return messages, nil
}

// handleNonMessageEvent handles Kafka events that are not messages
func (w *IngestionWorker) handleNonMessageEvent(event kafka.Event) {
	switch e := event.(type) {
	case kafka.Error:
		log.Printf("❌ Kafka error: %v", e)
	case kafka.OffsetsCommitted:
		log.Printf("📨 Received event: kafka.OffsetsCommitted")
		log.Printf("🤔 Unknown event type: kafka.OffsetsCommitted")
	default:
		log.Printf("📨 Received event: %T", event)
		log.Printf("🤔 Unknown event type: %T", event)
	}
}

// updateStateFromMessages processes all consumed messages to update blob state
func (w *IngestionWorker) updateStateFromMessages(ctx context.Context, messages []*kafka.Message) error {
	stateUpdates := 0

	for _, msg := range messages {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			log.Printf("📩 Processing Kafka message from topic %s, partition %d, offset %d",
				*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset)

			var event events.BlobStateEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("Failed to unmarshal blob state event: %v", err)
				continue
			}

			if w.updateBlobStateFromEvent(&event) {
				stateUpdates++
			}
		}
	}

	log.Printf("📊 Processed %d message(s), updated state for %d blob(s)", len(messages), stateUpdates)
	return nil
}

// updateBlobStateFromEvent updates the blob state from a single event, returns true if state was updated
func (w *IngestionWorker) updateBlobStateFromEvent(event *events.BlobStateEvent) bool {
	// Check if this blob should be processed by this worker (filtering + sharding)
	shouldProcess := w.blobFilter.ShouldProcess(
		event.BlobName,
		event.Subscription,
		event.Environment,
		event.LastModifiedDate,
		&w.config.Worker.Sharding,
	)

	if !shouldProcess {
		// Remove from tracking if we were tracking it but shouldn't anymore
		if _, exists := w.blobStates[event.BlobName]; exists {
			delete(w.blobStates, event.BlobName)
			log.Printf("Removed blob %s from tracking (no longer matches filters)", event.BlobName)
		}
		return false
	}

	// Get or update blob state - preserve existing state if we have it
	var blobState *BlobState
	var isNewBlob bool

	if existing, exists := w.blobStates[event.BlobName]; exists {
		// Update existing state but preserve progress
		blobState = existing
		blobState.State = event.Status
		blobState.LastUpdated = time.Now()

		// Only update LastOffset if the event has a higher value (never go backwards)
		if event.LastIngestedOffset > blobState.LastOffset {
			blobState.LastOffset = event.LastIngestedOffset
			log.Printf("Updated LastOffset for blob %s from %d to %d (from BlobState event)",
				event.BlobName, existing.LastOffset, event.LastIngestedOffset)
		} else {
			log.Printf("Keeping higher in-memory LastOffset for blob %s: %d >= %d (from BlobState event)",
				event.BlobName, blobState.LastOffset, event.LastIngestedOffset)
		}
	} else {
		// Create new state for first-time tracking
		blobState = &BlobState{
			BlobName:      event.BlobName,
			Subscription:  event.Subscription,
			Environment:   event.Environment,
			ContainerName: "commerce-logs-separated",
			State:         event.Status,
			LastOffset:    event.LastIngestedOffset,
			LastUpdated:   time.Now(),
		}
		isNewBlob = true
		log.Printf("Started tracking new blob %s with LastOffset %d", event.BlobName, event.LastIngestedOffset)
	}

	w.blobStates[event.BlobName] = blobState

	if isNewBlob {
		log.Printf("New blob state: %s (%s/%s) - %s",
			event.BlobName, event.Subscription, event.Environment, event.Status)
	} else {
		log.Printf("Updated blob state: %s (%s/%s) - %s",
			event.BlobName, event.Subscription, event.Environment, event.Status)
	}

	return true
}

// processAllTrackedBlobsAfterStateUpdate processes all tracked blobs after state updates with enhanced metrics
func (w *IngestionWorker) processAllTrackedBlobsAfterStateUpdate(ctx context.Context) error {
	iterationStart := time.Now()
	processingInterval := w.config.Worker.ProcessingConfig.LoopInterval

	// Apply debouncing logic at the very start of iteration
	timeSinceLastIteration := iterationStart.Sub(w.lastIterationStart)
	if !w.lastIterationStart.IsZero() && timeSinceLastIteration < processingInterval {
		waitTime := processingInterval - timeSinceLastIteration
		log.Printf("⏳ Debouncing: waiting %.1fs before starting next iteration (last iteration started %.1fs ago)",
			waitTime.Seconds(), timeSinceLastIteration.Seconds())

		// Wait for the remaining time or until context is cancelled
		select {
		case <-ctx.Done():
			log.Printf("🛑 Context cancelled during debounce wait")
			return ctx.Err()
		case <-time.After(waitTime):
			log.Printf("⏰ Debounce wait completed, proceeding with iteration")
		}
	}

	// Update iteration start time now that we're proceeding
	w.lastIterationStart = iterationStart

	// Process all tracked blobs
	return w.processAllTrackedBlobsWithProgress(ctx, iterationStart)
}

// countBlobsNeedingProcessing counts how many tracked blobs need processing
func (w *IngestionWorker) countBlobsNeedingProcessing() int {
	count := 0
	for _, blobState := range w.blobStates {
		if blobState.State == "open" || blobState.State == "closed" {
			count++
		}
	}
	return count
}

// processAllTrackedBlobsWithProgress processes all tracked blobs with progress indicators
func (w *IngestionWorker) processAllTrackedBlobsWithProgress(ctx context.Context, iterationStart time.Time) error {
	// Collect all blobs that need processing
	var blobsToProcess []*BlobState
	for _, blobState := range w.blobStates {
		if blobState.State == "open" || blobState.State == "closed" {
			blobsToProcess = append(blobsToProcess, blobState)
		}
	}

	totalBlobs := len(blobsToProcess)
	log.Printf("📋 Processing %d blob(s) in this iteration", totalBlobs)

	// Process each blob with progress tracking
	for i, blobState := range blobsToProcess {
		progress := fmt.Sprintf("[%d/%d]", i+1, totalBlobs)

		// Check for cancellation before each blob
		select {
		case <-ctx.Done():
			log.Printf("🛑 Context cancelled before processing blob %s %s", progress, blobState.BlobName)
			return ctx.Err()
		default:
			if err := w.processBlobWithMetrics(ctx, blobState, progress, iterationStart); err != nil {
				log.Printf("❌ Error processing blob %s %s: %v", progress, blobState.BlobName, err)
				// Continue processing other blobs
			}
		}
	}

	iterationDuration := time.Since(iterationStart)
	log.Printf("✅ Completed iteration in %.1fs - processed %d blob(s)", iterationDuration.Seconds(), totalBlobs)
	return nil
}

// processBlobWithMetrics processes a single blob with detailed timing metrics
func (w *IngestionWorker) processBlobWithMetrics(ctx context.Context, blob *BlobState, progress string, iterationStart time.Time) error {
	preProcessingDelay := time.Since(iterationStart)
	blobProcessingStart := time.Now()

	log.Printf("🚀 %s Processing blob: %s (delay before start: %.1fs)",
		progress, blob.BlobName, preProcessingDelay.Seconds())

	// Check for cancellation before processing
	select {
	case <-ctx.Done():
		log.Printf("🛑 Context cancelled before blob processing: %s", blob.BlobName)
		return ctx.Err()
	default:
		// Create blob processing info
		blobInfo := ingestion.BlobProcessingInfo{
			ContainerName: blob.ContainerName,
			BlobName:      blob.BlobName,
			StartOffset:   blob.LastOffset,
			Subscription:  blob.Subscription,
			Environment:   blob.Environment,
		}

		// Process the blob
		result, err := w.processor.ProcessBlob(ctx, blobInfo)
		if err != nil {
			return fmt.Errorf("failed to process blob %s: %w", blob.BlobName, err)
		}

		processingDuration := time.Since(blobProcessingStart)

		// Calculate metrics
		var downloadSpeed, writeLatency float64
		var speedText string
		var bytesProcessed int64
		if result != nil {
			bytesProcessed = result.ProcessedToOffset - blob.LastOffset
			if bytesProcessed > 0 && processingDuration > 0 {
				bytesPerSecond := float64(bytesProcessed) / processingDuration.Seconds()

				// Choose appropriate units for speed display
				if bytesPerSecond >= 1024*1024 {
					// Use MB/s for high speeds
					downloadSpeed = bytesPerSecond / (1024 * 1024)
					speedText = fmt.Sprintf("%.1f MB/s", downloadSpeed)
				} else if bytesPerSecond >= 1024 {
					// Use KB/s for medium speeds
					downloadSpeed = bytesPerSecond / 1024
					speedText = fmt.Sprintf("%.1f KB/s", downloadSpeed)
				} else {
					// Use B/s for very low speeds
					speedText = fmt.Sprintf("%.0f B/s", bytesPerSecond)
				}
			} else {
				speedText = "0 B/s"
			}

			// Estimate write latency as a portion of total processing time (rough approximation)
			if result.LinesProcessed > 0 {
				writeLatency = processingDuration.Seconds() * 0.1 // Assume ~10% of time spent on writes
			}
		} else {
			speedText = "N/A"
		}

		// Update state after successful processing
		if state, exists := w.blobStates[blob.BlobName]; exists {
			state.LastUpdated = time.Now()

			// Update the LastOffset to prevent reprocessing
			if result != nil && result.ProcessedToOffset > state.LastOffset {
				state.LastOffset = result.ProcessedToOffset
				log.Printf("📊 %s Completed blob: %s | Duration: %.1fs | Bytes: %s | Speed: %s | Lines: %d | Write latency: %.1fs",
					progress, blob.BlobName, processingDuration.Seconds(),
					formatBytes(bytesProcessed), speedText, result.LinesProcessed, writeLatency)
			} else if result != nil {
				log.Printf("📊 %s Completed blob: %s | Duration: %.1fs | No new data processed",
					progress, blob.BlobName, processingDuration.Seconds())
			}

			// If blob is closed and we've processed to the end, remove from tracking
			if state.State == "closed" {
				log.Printf("🔒 Blob %s is closed and fully processed, removing from tracking", blob.BlobName)
				delete(w.blobStates, blob.BlobName)
			}
		}
	}

	return nil
}

// formatBytes formats byte count in human-readable format
func formatBytes(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	} else if bytes < 1024*1024 {
		return fmt.Sprintf("%.1f KB", float64(bytes)/1024)
	} else if bytes < 1024*1024*1024 {
		return fmt.Sprintf("%.1f MB", float64(bytes)/(1024*1024))
	} else {
		return fmt.Sprintf("%.1f GB", float64(bytes)/(1024*1024*1024))
	}
}
