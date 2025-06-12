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

// BlobClosingProcessor handles blob closing detection independently of the main monitor
type BlobClosingProcessor struct {
	config        *blobConfig.Config
	kafkaProducer Producer
	eventChan     chan events.BlobObservedEvent
	stopChan      chan struct{}
}

// NewBlobClosingProcessor creates a new blob closing processor
func NewBlobClosingProcessor(config *blobConfig.Config, producer Producer) *BlobClosingProcessor {
	return &BlobClosingProcessor{
		config:        config,
		kafkaProducer: producer,
		eventChan:     make(chan events.BlobObservedEvent, 1000), // Buffered channel
		stopChan:      make(chan struct{}),
	}
}

// Start begins processing blob observed events for closing detection
func (blobClosingProcessor *BlobClosingProcessor) Start(ctx context.Context) {
	log.Printf("🔐 Starting blob closing processor")

	for {
		select {
		case <-ctx.Done():
			log.Printf("🔐 Blob closing processor stopped due to context cancellation")
			return
		case <-blobClosingProcessor.stopChan:
			log.Printf("🔐 Blob closing processor stopped")
			return
		case event := <-blobClosingProcessor.eventChan:
			blobClosingProcessor.processObservedEvent(event)
		}
	}
}

// Stop gracefully shuts down the processor
func (blobClosingProcessor *BlobClosingProcessor) Stop() {
	close(blobClosingProcessor.stopChan)
}

// ProcessBlobObserved queues a blob observed event for processing
func (blobClosingProcessor *BlobClosingProcessor) ProcessBlobObserved(event events.BlobObservedEvent) {
	select {
	case blobClosingProcessor.eventChan <- event:
		// Event queued successfully
	default:
		log.Printf("⚠️ Blob closing processor event queue full, dropping event for %s", event.BlobName)
	}
}

// Processes a single blob observed event to determine if blob should be closed
func (blobClosingProcessor *BlobClosingProcessor) processObservedEvent(event events.BlobObservedEvent) {
	now := time.Now().UTC()
	timeout := time.Duration(blobClosingProcessor.config.Global.BlobClosingConfig.TimeoutMinutes) * time.Minute

	// Check if the blob should be closed (simple timeout check)
	if now.Sub(event.LastModifiedDate) >= timeout {
		closedEvent := events.BlobClosedEvent{
			Subscription:     event.Subscription,
			Environment:      event.Environment,
			BlobName:         event.BlobName,
			ServiceSelector:  event.ServiceSelector,
			LastModifiedDate: event.LastModifiedDate,
			ClosedDate:       now,
			SizeInBytes:      event.SizeInBytes,
			TimeoutMinutes:   blobClosingProcessor.config.Global.BlobClosingConfig.TimeoutMinutes,
		}

		// Publish the closed event
		if err := blobClosingProcessor.publishBlobClosedEvent(closedEvent); err != nil {
			log.Printf("❌ Failed to publish BlobClosed event for %s: %v", closedEvent.BlobName, err)
		} else {
			log.Printf("🔐 Blob closed: %s (inactive for %v)", closedEvent.BlobName, timeout)
		}
	}
}

// Publishes blob closed events to notify downstream consumers
func (blobClosingProcessor *BlobClosingProcessor) publishBlobClosedEvent(event events.BlobClosedEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal BlobClosed event: %w", err)
	}

	key := sharedEvents.GenerateBlobEventKey(event.Subscription, event.Environment, "closed", event.BlobName)

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &blobClosingProcessor.config.Kafka.Topic,
		},
		Key:   []byte(key),
		Value: eventJSON,
	}

	return blobClosingProcessor.kafkaProducer.Produce(message, nil)
}
