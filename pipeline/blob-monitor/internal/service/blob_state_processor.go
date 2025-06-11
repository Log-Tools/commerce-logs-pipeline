package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	blobConfig "github.com/Log-Tools/commerce-logs-pipeline/pipeline/blob-monitor/internal/config"
	"github.com/Log-Tools/commerce-logs-pipeline/pipeline/blob-monitor/internal/events"
	sharedEvents "github.com/Log-Tools/commerce-logs-pipeline/pipeline/events"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// BlobStateProcessor maintains blob state in a compacted topic by consuming events
type BlobStateProcessor struct {
	config     *blobConfig.Config
	consumer   *kafka.Consumer
	producer   Producer
	stopChan   chan struct{}
	blobStates map[string]*sharedEvents.BlobStateEvent // in-memory state for processing
}

// NewBlobStateProcessor creates a new blob state processor
func NewBlobStateProcessor(config *blobConfig.Config, producer Producer) (*BlobStateProcessor, error) {
	// Create Kafka consumer for Ingestion.Blobs topic
	consumerConfig := &kafka.ConfigMap{
		"bootstrap.servers":  config.Kafka.Brokers,
		"group.id":           "blob-state-processor",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false, // We'll commit manually for better control
	}

	consumer, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	return &BlobStateProcessor{
		config:     config,
		consumer:   consumer,
		producer:   producer,
		stopChan:   make(chan struct{}),
		blobStates: make(map[string]*sharedEvents.BlobStateEvent),
	}, nil
}

// Start begins processing blob events to maintain state
func (processor *BlobStateProcessor) Start(ctx context.Context) error {
	log.Printf("🗂️ Starting blob state processor")

	// Subscribe to the blob events topic
	err := processor.consumer.Subscribe(sharedEvents.TopicBlobs, nil)
	if err != nil {
		return fmt.Errorf("failed to subscribe to %s: %w", sharedEvents.TopicBlobs, err)
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("🗂️ Blob state processor stopped due to context cancellation")
			return nil
		case <-processor.stopChan:
			log.Printf("🗂️ Blob state processor stopped")
			return nil
		default:
			// Poll for messages
			msg, err := processor.consumer.ReadMessage(1 * time.Second)
			if err != nil {
				// Check if it's a timeout (expected) or a real error
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
					continue // Normal timeout, keep polling
				}
				log.Printf("❌ Error reading message: %v", err)
				continue
			}

			if err := processor.processMessage(msg); err != nil {
				log.Printf("❌ Error processing message: %v", err)
				// Continue processing other messages even if one fails
			}

			// Commit the message after successful processing
			if _, err := processor.consumer.CommitMessage(msg); err != nil {
				log.Printf("❌ Error committing message: %v", err)
			}
		}
	}
}

// Stop gracefully shuts down the processor
func (processor *BlobStateProcessor) Stop() {
	close(processor.stopChan)
	if processor.consumer != nil {
		processor.consumer.Close()
	}
}

// Processes a single message from the blob events topic
func (processor *BlobStateProcessor) processMessage(msg *kafka.Message) error {
	// Try to determine event type based on the message content
	var eventType string
	var err error

	// Parse the message to determine event type
	var rawEvent map[string]interface{}
	if err := json.Unmarshal(msg.Value, &rawEvent); err != nil {
		return fmt.Errorf("failed to parse message: %w", err)
	}

	// Determine event type based on fields present
	if _, hasClosedDate := rawEvent["closedDate"]; hasClosedDate {
		eventType = "closed"
	} else if _, hasDate := rawEvent["date"]; hasDate {
		eventType = "listed"
	} else if _, hasObservationDate := rawEvent["observationDate"]; hasObservationDate {
		eventType = "observed"
	} else if _, hasProcessedFromOffset := rawEvent["processedFromOffset"]; hasProcessedFromOffset {
		eventType = "completion"
	} else {
		return fmt.Errorf("unknown event type in message")
	}

	switch eventType {
	case "observed":
		var event events.BlobObservedEvent
		if err = json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("failed to unmarshal BlobObservedEvent: %w", err)
		}
		return processor.processBlobObserved(event)

	case "closed":
		var event events.BlobClosedEvent
		if err = json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("failed to unmarshal BlobClosedEvent: %w", err)
		}
		return processor.processBlobClosed(event)

	case "completion":
		var event sharedEvents.BlobCompletionEvent
		if err = json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("failed to unmarshal BlobCompletionEvent: %w", err)
		}
		return processor.processBlobCompletion(event)

	case "listed":
		// BlobsListedEvent doesn't affect individual blob state, so we ignore it
		return nil

	default:
		return fmt.Errorf("unsupported event type: %s", eventType)
	}
}

// Processes a blob observed event to update blob state
func (processor *BlobStateProcessor) processBlobObserved(event events.BlobObservedEvent) error {
	blobKey := processor.getBlobKey(event.Subscription, event.Environment, event.BlobName)

	// Get or create blob state
	state, exists := processor.blobStates[blobKey]
	if !exists {
		state = &sharedEvents.BlobStateEvent{
			Subscription:       event.Subscription,
			Environment:        event.Environment,
			BlobName:           event.BlobName,
			Status:             "open",
			FirstObservedDate:  event.ObservationDate,
			CreationDate:       event.CreationDate,
			TotalLinesIngested: 0,
			LastIngestedOffset: 0,
		}
		processor.blobStates[blobKey] = state
	}

	// Update state with observed event data
	state.SizeInBytes = event.SizeInBytes
	state.LastModifiedDate = event.LastModifiedDate
	state.LastObservedDate = event.ObservationDate
	state.ServiceSelector = event.ServiceSelector
	state.LastUpdated = time.Now().UTC()

	// Update creation date if we didn't have it before
	if state.CreationDate.IsZero() && !event.CreationDate.IsZero() {
		state.CreationDate = event.CreationDate
	}

	// Only change to open if not already closed
	if state.Status != "closed" {
		state.Status = "open"
	}

	return processor.publishBlobState(*state)
}

// Processes a blob closed event to update blob state
func (processor *BlobStateProcessor) processBlobClosed(event events.BlobClosedEvent) error {
	blobKey := processor.getBlobKey(event.Subscription, event.Environment, event.BlobName)

	// Get or create blob state
	state, exists := processor.blobStates[blobKey]
	if !exists {
		// Create new state if it doesn't exist (shouldn't happen normally)
		state = &sharedEvents.BlobStateEvent{
			Subscription:      event.Subscription,
			Environment:       event.Environment,
			BlobName:          event.BlobName,
			Status:            "closed",         // Start as closed since that's what triggered this
			FirstObservedDate: time.Now().UTC(), // Best guess
			// CreationDate unknown in closed event input
			TotalLinesIngested: 0,
			LastIngestedOffset: 0,
		}
		processor.blobStates[blobKey] = state
	}

	// Opaque event handling - just change status to closed
	state.Status = "closed"
	state.LastUpdated = time.Now().UTC()

	return processor.publishBlobState(*state)
}

// Processes a blob completion event to update ingestion progress
func (processor *BlobStateProcessor) processBlobCompletion(event sharedEvents.BlobCompletionEvent) error {
	blobKey := processor.getBlobKey(event.Subscription, event.Environment, event.BlobName)

	// Get existing blob state
	state, exists := processor.blobStates[blobKey]
	if !exists {
		// Create new state if it doesn't exist
		state = &sharedEvents.BlobStateEvent{
			Subscription:      event.Subscription,
			Environment:       event.Environment,
			BlobName:          event.BlobName,
			Status:            "open",
			FirstObservedDate: event.ProcessedDate,
			// CreationDate unknown in completion event input
			TotalLinesIngested: 0,
			LastIngestedOffset: 0,
		}
		processor.blobStates[blobKey] = state
	}

	// Update ingestion progress
	if event.ProcessedToEndOffset > state.LastIngestedOffset {
		state.LastIngestedOffset = event.ProcessedToEndOffset
	}
	state.TotalLinesIngested += event.LinesSent
	state.LastProcessedDate = &event.ProcessedDate
	state.LastUpdated = time.Now().UTC()

	return processor.publishBlobState(*state)
}

// Publishes blob state to the compacted topic
func (processor *BlobStateProcessor) publishBlobState(state sharedEvents.BlobStateEvent) error {
	stateJSON, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal blob state: %w", err)
	}

	// Key is subscription:environment:blobname for compaction
	key := processor.getBlobKey(state.Subscription, state.Environment, state.BlobName)

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &[]string{sharedEvents.TopicBlobState}[0],
		},
		Key:   []byte(key),
		Value: stateJSON,
	}

	return processor.producer.Produce(message, nil)
}

// Generates a consistent blob key for compaction
func (processor *BlobStateProcessor) getBlobKey(subscription, environment, blobName string) string {
	return fmt.Sprintf("%s:%s:%s", subscription, environment, blobName)
}
