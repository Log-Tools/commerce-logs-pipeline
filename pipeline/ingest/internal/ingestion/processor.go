package ingestion

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Log-Tools/commerce-logs-pipeline/ingest/internal/config"
	"github.com/Log-Tools/commerce-logs-pipeline/pipeline/events"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// blobProcessor implements BlobProcessor interface
type blobProcessor struct {
	config         *config.Config
	producer       Producer
	storageFactory StorageClientFactory
}

// NewBlobProcessor creates a new blob processor
func NewBlobProcessor(cfg *config.Config, producer Producer, storageFactory StorageClientFactory) BlobProcessor {
	bp := &blobProcessor{
		config:         cfg,
		producer:       producer,
		storageFactory: storageFactory,
	}

	// Start a background goroutine to log delivery reports so we can see real broker errors.
	go bp.handleDeliveryEvents(bp.producer.Events())

	return bp
}

// ProcessBlob processes a single blob from a given offset
func (p *blobProcessor) ProcessBlob(ctx context.Context, blobInfo BlobProcessingInfo) (*BlobProcessingResult, error) {
	log.Printf("Starting processing for blob: %s/%s, offset: %d",
		blobInfo.ContainerName, blobInfo.BlobName, blobInfo.StartOffset)

	// Create blob client
	blobClient, err := p.storageFactory.CreateBlobClient(blobInfo.Subscription, blobInfo.Environment, blobInfo.ContainerName, blobInfo.BlobName)
	if err != nil {
		return nil, fmt.Errorf("failed to create blob client: %w", err)
	}

	// Get blob properties
	props, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get blob properties for %s: %w", blobInfo.BlobName, err)
	}

	currentBlobTotalSize := *props.ContentLength
	log.Printf("Blob %s current total size: %d bytes", blobInfo.BlobName, currentBlobTotalSize)

	// Send BlobObserved event
	err = p.sendBlobObservedMessage(ctx, blobInfo.BlobName, blobInfo.Subscription,
		blobInfo.Environment, currentBlobTotalSize, *props.LastModified)
	if err != nil {
		log.Printf("⚠️ Failed to send BlobObserved message for %s: %v", blobInfo.BlobName, err)
		// Don't fail the entire process for this, just log the warning
	}

	// Validate start offset
	if blobInfo.StartOffset < 0 {
		return nil, fmt.Errorf("startOffset (%d) cannot be negative", blobInfo.StartOffset)
	}

	// Check if there's new data to process
	if blobInfo.StartOffset >= currentBlobTotalSize {
		log.Printf("Start offset %d is at or beyond current end of blob %s (size %d). No new data to process.",
			blobInfo.StartOffset, blobInfo.BlobName, currentBlobTotalSize)
		err := p.sendCompletionMessage(ctx, blobInfo.BlobName, blobInfo.StartOffset,
			currentBlobTotalSize, 0, blobInfo.Subscription, blobInfo.Environment)
		if err != nil {
			return nil, err
		}
		return &BlobProcessingResult{
			ProcessedToOffset: currentBlobTotalSize,
			LinesProcessed:    0,
		}, nil
	}

	// Download blob segment
	downloadOptions := blob.DownloadStreamOptions{
		Range: blob.HTTPRange{Offset: blobInfo.StartOffset},
	}

	log.Printf("Downloading from offset %d to current end of blob %s", blobInfo.StartOffset, blobInfo.BlobName)

	downloadResp, err := blobClient.DownloadStream(ctx, &downloadOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to download blob %s: %w", blobInfo.BlobName, err)
	}
	defer downloadResp.Body.Close()

	actualDownloadedLength := *downloadResp.ContentLength
	log.Printf("Successfully initiated download. Actual stream ContentLength: %s.", formatBytes(actualDownloadedLength))

	if actualDownloadedLength == 0 {
		log.Printf("Downloaded 0 bytes. No new content to process from offset %d for blob %s.",
			blobInfo.StartOffset, blobInfo.BlobName)
		err := p.sendCompletionMessage(ctx, blobInfo.BlobName, blobInfo.StartOffset,
			currentBlobTotalSize, 0, blobInfo.Subscription, blobInfo.Environment)
		if err != nil {
			return nil, err
		}
		return &BlobProcessingResult{
			ProcessedToOffset: currentBlobTotalSize,
			LinesProcessed:    0,
		}, nil
	}

	// Process the downloaded content
	lineCount, err := p.processDownloadedContent(ctx, downloadResp.Body, blobInfo, actualDownloadedLength)
	if err != nil {
		return nil, fmt.Errorf("failed to process downloaded content: %w", err)
	}

	// Send completion message
	err = p.sendCompletionMessage(ctx, blobInfo.BlobName, blobInfo.StartOffset,
		currentBlobTotalSize, lineCount, blobInfo.Subscription, blobInfo.Environment)
	if err != nil {
		return nil, fmt.Errorf("failed to send completion message: %w", err)
	}

	log.Printf("Successfully processed blob segment %s/%s from offset %d up to %d. Sent %d lines.",
		blobInfo.ContainerName, blobInfo.BlobName, blobInfo.StartOffset, currentBlobTotalSize, lineCount)

	return &BlobProcessingResult{
		ProcessedToOffset: currentBlobTotalSize,
		LinesProcessed:    lineCount,
	}, nil
}

// processDownloadedContent handles the actual content processing
func (p *blobProcessor) processDownloadedContent(ctx context.Context, body io.ReadCloser,
	blobInfo BlobProcessingInfo, downloadedLength int64) (int, error) {

	// Create gzip reader
	gzipReader, err := gzip.NewReader(body)
	if err != nil {
		if (err == io.EOF || err == io.ErrUnexpectedEOF) && downloadedLength > 0 {
			log.Printf("Gzip reader encountered EOF with %d bytes downloaded. Empty or truncated segment from offset %d for blob %s.",
				downloadedLength, blobInfo.StartOffset, blobInfo.BlobName)
			return 0, nil
		}
		return 0, fmt.Errorf("failed to create gzip reader for blob %s (offset %d): %w",
			blobInfo.BlobName, blobInfo.StartOffset, err)
	}
	defer gzipReader.Close()

	// Process lines without delivery channels to avoid "send on closed channel" panics
	scanner := bufio.NewScanner(gzipReader)
	buffer := make([]byte, 0, p.config.Worker.ProcessingConfig.LineBufferSize)
	scanner.Buffer(buffer, p.config.Worker.ProcessingConfig.LineBufferSize)

	lineCounter := 0
	// Flush much less frequently to reduce contention; 1 000 lines by default for single-replica cluster.
	const flushInterval = 1000

	for scanner.Scan() {
		line := scanner.Text()
		lineCounter++

		kafkaKey := events.GenerateBlobEventKey(blobInfo.Subscription, blobInfo.Environment,
			fmt.Sprintf("line-%d", lineCounter), blobInfo.BlobName)

		message := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &p.config.Kafka.IngestTopic, Partition: -1},
			Key:            []byte(kafkaKey),
			Value:          []byte(line),
		}

		// Use nil delivery channel to avoid channel management issues
		err = p.producer.Produce(message, nil)
		if err != nil {
			log.Printf("⚠️ Failed to enqueue message to Kafka (key: %s): %v.", kafkaKey, err)
		}

		// Periodically flush so we don't grow memory unbounded, but treat timeouts as warnings.
		if lineCounter%flushInterval == 0 {
			remainingMessages := p.producer.Flush(p.config.Kafka.Producer.FlushTimeoutMs)
			if remainingMessages > 0 {
				log.Printf("⚠️ %d messages still pending after periodic flush (line %d) for blob %s; continuing",
					remainingMessages, lineCounter, blobInfo.BlobName)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("error reading unzipped content from blob %s (offset %d): %w",
			blobInfo.BlobName, blobInfo.StartOffset, err)
	}

	log.Printf("Finished reading lines from blob %s (offset %d). Total lines in this segment: %d",
		blobInfo.BlobName, blobInfo.StartOffset, lineCounter)

	// Final flush for any remaining messages (less than flushInterval)
	remainingToFlush := lineCounter % flushInterval
	if remainingToFlush > 0 {
		log.Printf("Final flush for remaining %d messages...", remainingToFlush)
		remainingMessages := p.producer.Flush(p.config.Kafka.Producer.FlushTimeoutMs)
		if remainingMessages > 0 {
			log.Printf("⚠️ %d messages were not delivered after final flush timeout for blob %s (offset %d)",
				remainingMessages, blobInfo.BlobName, blobInfo.StartOffset)
			return 0, fmt.Errorf("%d messages not delivered from Kafka producer for blob %s (offset %d)",
				remainingMessages, blobInfo.BlobName, blobInfo.StartOffset)
		}
	}

	log.Printf("✅ All messages for segment %s (offset %d) delivered successfully. Total: %d lines",
		blobInfo.BlobName, blobInfo.StartOffset, lineCounter)

	return lineCounter, nil
}

// handleDeliveryEvents handles Kafka delivery events
func (p *blobProcessor) handleDeliveryEvents(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("❌ Delivery failed for message to %s (key: %s): %v",
					*ev.TopicPartition.Topic, string(ev.Key), ev.TopicPartition.Error)
			}
		case kafka.Error:
			log.Printf("❌ Kafka producer error: %v", ev)
		}
	}
}

// sendCompletionMessage sends a completion message to Kafka
func (p *blobProcessor) sendCompletionMessage(ctx context.Context, blobName string,
	processedFromOffset, processedToEndOffset int64, linesSentInSegment int,
	subscription, environment string) error {

	completionEvent := events.BlobCompletionEvent{
		BlobName:             blobName,
		ProcessedFromOffset:  processedFromOffset,
		ProcessedToEndOffset: processedToEndOffset,
		LinesSent:            linesSentInSegment,
		ProcessedDate:        time.Now(),
		Subscription:         subscription,
		Environment:          environment,
	}

	messageBytes, err := json.Marshal(completionEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal completion event: %w", err)
	}

	kafkaKey := events.GenerateBlobEventKey(subscription, environment,
		fmt.Sprintf("offset%d-completion", processedFromOffset), blobName)

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.config.Kafka.BlobsTopic, Partition: -1},
		Key:            []byte(kafkaKey),
		Value:          messageBytes,
	}

	// Use nil delivery channel to avoid "send on closed channel" panics
	err = p.producer.Produce(message, nil)
	if err != nil {
		return fmt.Errorf("failed to produce completion message: %w", err)
	}

	// Flush to ensure completion message is persisted before we return success
	remainingMessages := p.producer.Flush(p.config.Kafka.Producer.FlushTimeoutMs)
	if remainingMessages > 0 {
		return fmt.Errorf("completion message for blob %s (offset %d) was not delivered within timeout",
			blobName, processedFromOffset)
	}

	log.Printf("✅ Completion message sent and confirmed for blob %s (offset %d)", blobName, processedFromOffset)
	return nil
}

// sendBlobObservedMessage sends a blob observed message to Kafka
func (p *blobProcessor) sendBlobObservedMessage(ctx context.Context, blobName, subscription,
	environment string, sizeInBytes int64, lastModifiedDate time.Time) error {

	observedEvent := events.BlobObservedEvent{
		BlobName:         blobName,
		SizeInBytes:      sizeInBytes,
		LastModifiedDate: lastModifiedDate,
		ObservationDate:  time.Now(),
		Subscription:     subscription,
		Environment:      environment,
	}

	messageBytes, err := json.Marshal(observedEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal observed event: %w", err)
	}

	kafkaKey := events.GenerateBlobEventKey(subscription, environment, "observed", blobName)

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.config.Kafka.BlobsTopic, Partition: -1},
		Key:            []byte(kafkaKey),
		Value:          messageBytes,
	}

	// Use nil delivery channel to avoid "send on closed channel" panics
	err = p.producer.Produce(message, nil)
	if err != nil {
		return fmt.Errorf("failed to produce observed message: %w", err)
	}

	log.Printf("✅ Blob observed message sent for %s (%s)", blobName, formatBytes(sizeInBytes))
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
