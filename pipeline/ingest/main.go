package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Log-Tools/commerce-logs-pipeline/config"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// KafkaCompletionMessage defines the structure for the final completion event.
// The ProcessedToEndOffset indicates the byte offset in the blob up to which
// processing has been successfully completed for this run. For a full segment process,
// this will be the total size of the blob at the time of processing.
type KafkaCompletionMessage struct {
	BlobName             string `json:"blobName"`
	ProcessedFromOffset  int64  `json:"processedFromOffset"`
	ProcessedToEndOffset int64  `json:"processedToEndOffset"`
	LinesSent            int    `json:"linesSent"`
}

type BlobObservedMessage struct {
	Subscription     string    `json:"subscription"`
	Environment      string    `json:"environment"`
	BlobName         string    `json:"blobName"`
	ObservationDate  time.Time `json:"observationDate"`
	SizeInBytes      int64     `json:"sizeInBytes"`
	LastModifiedDate time.Time `json:"lastModifiedDate"`
}

// ProcessBlobSegmentToKafka downloads a gzipped blob segment from Azure, ungzips it,
// and sends its lines to a Kafka topic. A final completion message is sent
// after all lines from the segment are processed.
func ProcessBlobSegmentToKafka(
	ctx context.Context,
	storageConfig config.StorageAccount,
	azureContainerName string,
	azureBlobName string,
	startOffset int64,
	kafkaBrokers string, // Comma-separated list
	kafkaTopic string,
	subscriptionID string,
	environment string,
) error {
	log.Printf("Starting processing for blob: %s/%s, offset: %d", azureContainerName, azureBlobName, startOffset)

	// 1. Initialize Azure Blob Client
	cred, err := azblob.NewSharedKeyCredential(storageConfig.AccountName, storageConfig.AccessKey)
	if err != nil {
		return fmt.Errorf("failed to create Azure shared key credential: %w", err)
	}

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", storageConfig.AccountName)
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	if err != nil {
		return fmt.Errorf("failed to create Azure Blob client: %w", err)
	}

	containerClient := client.ServiceClient().NewContainerClient(azureContainerName)
	blobClient := containerClient.NewBlobClient(azureBlobName)

	// 2. Get Blob Properties
	props, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to get blob properties for %s: %w", azureBlobName, err)
	}
	currentBlobTotalSize := *props.ContentLength // This is the offset we've processed up to in this run
	log.Printf("Blob %s current total size: %d bytes", azureBlobName, currentBlobTotalSize)

	// Send BlobObserved event
	err = sendBlobObservedMessage(ctx, kafkaBrokers, azureBlobName, subscriptionID, environment, currentBlobTotalSize, *props.LastModified, nil, nil)
	if err != nil {
		log.Printf("⚠️ Failed to send BlobObserved message for %s: %v", azureBlobName, err)
		// Don't fail the entire process for this, just log the warning
	}

	if startOffset < 0 {
		return fmt.Errorf("startOffset (%d) cannot be negative", startOffset)
	}
	// If startOffset is beyond the current known size, it means no new data.
	// Send a completion message reflecting this.
	if startOffset > currentBlobTotalSize {
		log.Printf("Start offset %d is beyond the current end of blob %s (size %d). Nothing to process.", startOffset, azureBlobName, currentBlobTotalSize)
		// ProcessedToEndOffset should reflect the known size of the blob if startOffset was beyond it.
		// Or, if we want to say "we attempted to read from X and found nothing new up to Y",
		// currentBlobTotalSize is the most accurate "end" we know.
		return sendCompletionMessage(ctx, kafkaBrokers, "Ingestion.Blobs", azureBlobName, startOffset, currentBlobTotalSize, 0, nil, nil)
	}
	// If startOffset is exactly at the end, no new data to download.
	if startOffset == currentBlobTotalSize && currentBlobTotalSize > 0 { // Check currentBlobTotalSize > 0 for empty blobs
		log.Printf("Start offset %d is at the current end of blob %s (size %d). No new data to process.", startOffset, azureBlobName, currentBlobTotalSize)
		return sendCompletionMessage(ctx, kafkaBrokers, "Ingestion.Blobs", azureBlobName, startOffset, currentBlobTotalSize, 0, nil, nil)
	}

	// 3. Determine download range
	// To download from startOffset to the current end of the blob,
	// set Offset and leave Count as nil.
	downloadOptions := azblob.DownloadStreamOptions{
		Range: azblob.HTTPRange{Offset: startOffset},
	}
	log.Printf("Attempting to download from offset %d to current end of blob %s", startOffset, azureBlobName)

	// 4. Download Blob Segment
	downloadResp, err := blobClient.DownloadStream(ctx, &downloadOptions)
	if err != nil {
		return fmt.Errorf("failed to download blob %s: %w", azureBlobName, err)
	}
	defer downloadResp.Body.Close()

	actualDownloadedLength := *downloadResp.ContentLength
	log.Printf("Successfully initiated download. Actual stream ContentLength: %d bytes.", actualDownloadedLength)

	if actualDownloadedLength == 0 {
		log.Printf("Downloaded 0 bytes. No new content to process from offset %d for blob %s.", startOffset, azureBlobName)
		// Send completion message. ProcessedToEndOffset is currentBlobTotalSize,
		// as that's the end of the blob content known before this download attempt.
		return sendCompletionMessage(ctx, kafkaBrokers, "Ingestion.Blobs", azureBlobName, startOffset, currentBlobTotalSize, 0, nil, nil)
	}

	// 5. UnGzip Stream
	gzipReader, err := gzip.NewReader(downloadResp.Body)
	if err != nil {
		if (err == io.EOF || err == io.ErrUnexpectedEOF) && actualDownloadedLength > 0 {
			log.Printf("Gzip reader encountered EOF or UnexpectedEOF with %d bytes downloaded. This might indicate an empty or truncated gzipped stream segment from offset %d for blob %s. Sending completion as if 0 lines.", actualDownloadedLength, startOffset, azureBlobName)
			return sendCompletionMessage(ctx, kafkaBrokers, "Ingestion.Blobs", azureBlobName, startOffset, currentBlobTotalSize, 0, nil, nil)
		}
		return fmt.Errorf("failed to create gzip reader for blob %s (offset %d): %w", azureBlobName, startOffset, err)
	}
	defer gzipReader.Close()

	// 6. Initialize Kafka Producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers,
		"acks":              "all",
	})
	if err != nil {
		return fmt.Errorf("failed to create Kafka producer: %w", err)
	}
	defer producer.Close()

	deliveryChan := make(chan kafka.Event, 10000)
	go func() {
		for e := range deliveryChan {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("❌ Delivery failed for message to %s (key: %s): %v", *ev.TopicPartition.Topic, string(ev.Key), ev.TopicPartition.Error)
				}
			case kafka.Error:
				log.Printf("❌ Kafka producer error: %v", ev)
			}
		}
	}()

	// 7. Process Lines and Produce to Kafka
	scanner := bufio.NewScanner(gzipReader)
	// Increase buffer size to handle long log lines (default is 64KB, set to 1MB)
	buffer := make([]byte, 0, 1024*1024) // 1MB buffer
	scanner.Buffer(buffer, 1024*1024)    // 1MB max token size
	lineCounter := 0
	for scanner.Scan() {
		line := scanner.Text()
		lineCounter++
		kafkaKey := fmt.Sprintf("%s-%d", azureBlobName, lineCounter)
		message := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
			Key:            []byte(kafkaKey),
			Value:          []byte(line),
		}
		err = producer.Produce(message, deliveryChan)
		if err != nil {
			log.Printf("⚠️ Failed to enqueue message to Kafka (key: %s): %v.", kafkaKey, err)
		}
		if lineCounter%1000 == 0 {
			log.Printf("Produced %d lines from %s (offset %d) to Kafka topic %s...", lineCounter, azureBlobName, startOffset, kafkaTopic)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading unzipped content from blob %s (offset %d): %w", azureBlobName, startOffset, err)
	}

	log.Printf("Finished reading lines from blob %s (offset %d). Total lines in this segment: %d", azureBlobName, startOffset, lineCounter)

	// 8. Produce Completion Event
	// The `currentBlobTotalSize` is the offset up to which we've processed in this run.
	// This reflects the size of the blob *before* this specific download operation began.
	// If the blob grew during the download, currentBlobTotalSize still represents the "known end"
	// when the decision to download this segment was made. The next run would pick up from this point.
	err = sendCompletionMessage(ctx, kafkaBrokers, "Ingestion.Blobs", azureBlobName, startOffset, currentBlobTotalSize, lineCounter, producer, deliveryChan)
	if err != nil {
		return fmt.Errorf("failed to send completion message for blob %s (offset %d): %w", azureBlobName, startOffset, err)
	}

	log.Printf("Flushing Kafka producer for blob %s (offset %d)...", azureBlobName, startOffset)
	remainingMessages := producer.Flush(30 * 1000)
	if remainingMessages > 0 {
		log.Printf("⚠️ %d messages were not delivered after flush timeout for blob %s (offset %d)", remainingMessages, azureBlobName, startOffset)
		return fmt.Errorf("%d messages not delivered from Kafka producer for blob %s (offset %d)", remainingMessages, azureBlobName, startOffset)
	}
	log.Printf("✅ Kafka producer flushed successfully. All messages for segment %s (offset %d) sent.", azureBlobName, startOffset)

	log.Printf("Successfully processed blob segment %s/%s from offset %d up to %d. Sent %d lines and 1 completion message.",
		azureContainerName, azureBlobName, startOffset, currentBlobTotalSize, lineCounter)

	return nil
}

// sendCompletionMessage helper function to construct and send the completion message.
func sendCompletionMessage(
	_ context.Context,
	kafkaBrokers string,
	kafkaTopic string,
	blobName string,
	processedFromOffset int64,
	processedToEndOffset int64, // This is the key change, reflecting the end of the processed segment
	linesSentInSegment int,
	existingProducer *kafka.Producer,
	existingDeliveryChan chan kafka.Event,
) error {
	completionMsg := KafkaCompletionMessage{
		BlobName:             blobName,
		ProcessedFromOffset:  processedFromOffset,
		ProcessedToEndOffset: processedToEndOffset, // Use the new field name
		LinesSent:            linesSentInSegment,
	}
	completionMsgJSON, err := json.Marshal(completionMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal completion message for %s (from %d to %d): %w", blobName, processedFromOffset, processedToEndOffset, err)
	}

	// Key uses ProcessedFromOffset to identify the segment this completion message belongs to.
	kafkaKeyCompletion := fmt.Sprintf("%s-offset%d-completion", blobName, processedFromOffset)
	kafkaMessageCompletion := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
		Key:            []byte(kafkaKeyCompletion),
		Value:          completionMsgJSON,
	}

	var p *kafka.Producer = existingProducer
	var deliveryChan chan kafka.Event = existingDeliveryChan
	var tempProducerCreated bool = false

	if p == nil {
		tempP, tempErr := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBrokers, "acks": "all"})
		if tempErr != nil {
			return fmt.Errorf("failed to create temporary Kafka producer for completion message (blob %s, from %d to %d): %w", blobName, processedFromOffset, processedToEndOffset, tempErr)
		}
		defer tempP.Close()
		p = tempP
		tempProducerCreated = true

		// Setup temporary delivery channel and handler for the temporary producer
		tempDeliveryChan := make(chan kafka.Event, 1) // Small buffer for one message
		defer close(tempDeliveryChan)                 // Ensure this channel is closed

		go func() {
			// This goroutine will exit once tempDeliveryChan is closed.
			for e := range tempDeliveryChan {
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						log.Printf("❌ Delivery failed for completion message via temp producer (key: %s): %v", string(ev.Key), ev.TopicPartition.Error)
					} else {
						log.Printf("✅ Delivered completion message via temp producer (key: %s) to %s [%d] at offset %v",
							string(ev.Key), *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
					}
				}
			}
		}()
		deliveryChan = tempDeliveryChan
	}

	log.Printf("Producing completion message for %s (from offset %d to %d) to Kafka topic %s: %s", blobName, processedFromOffset, processedToEndOffset, kafkaTopic, string(completionMsgJSON))
	err = p.Produce(kafkaMessageCompletion, deliveryChan)
	if err != nil {
		return fmt.Errorf("failed to enqueue completion message (key: %s): %w", kafkaKeyCompletion, err)
	}

	if tempProducerCreated { // If using a temporary producer, flush it now.
		if remaining := p.Flush(15 * 1000); remaining > 0 {
			return fmt.Errorf("failed to deliver completion message via temp producer, %d messages remaining after flush (key: %s)", remaining, kafkaKeyCompletion)
		}
		log.Printf("✅ Completion message (key: %s) flushed successfully using temporary producer.", kafkaKeyCompletion)
	}
	// If using existingProducer, the main function's Flush will handle this message.

	return nil
}

// sendBlobObservedMessage helper function to construct and send the blob observed message.
func sendBlobObservedMessage(
	_ context.Context,
	kafkaBrokers string,
	blobName string,
	subscription string,
	environment string,
	sizeInBytes int64,
	lastModifiedDate time.Time,
	existingProducer *kafka.Producer,
	existingDeliveryChan chan kafka.Event,
) error {
	blobObservedMsg := BlobObservedMessage{
		Subscription:     subscription,
		Environment:      environment,
		BlobName:         blobName,
		ObservationDate:  time.Now(),
		SizeInBytes:      sizeInBytes,
		LastModifiedDate: lastModifiedDate,
	}
	blobObservedMsgJSON, err := json.Marshal(blobObservedMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal blob observed message for %s: %w", blobName, err)
	}

	kafkaKeyObserved := fmt.Sprintf("%s-observed", blobName)
	kafkaMessageObserved := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &[]string{"Ingestion.Blobs"}[0], Partition: kafka.PartitionAny},
		Key:            []byte(kafkaKeyObserved),
		Value:          blobObservedMsgJSON,
	}

	var p *kafka.Producer = existingProducer
	var deliveryChan chan kafka.Event = existingDeliveryChan
	var tempProducerCreated bool = false

	if p == nil {
		tempP, tempErr := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBrokers, "acks": "all"})
		if tempErr != nil {
			return fmt.Errorf("failed to create temporary Kafka producer for blob observed message (blob %s): %w", blobName, tempErr)
		}
		defer tempP.Close()
		p = tempP
		tempProducerCreated = true

		// Setup temporary delivery channel and handler for the temporary producer
		tempDeliveryChan := make(chan kafka.Event, 1) // Small buffer for one message
		defer close(tempDeliveryChan)                 // Ensure this channel is closed

		go func() {
			for e := range tempDeliveryChan {
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						log.Printf("❌ Delivery failed for blob observed message via temp producer (key: %s): %v", string(ev.Key), ev.TopicPartition.Error)
					} else {
						log.Printf("✅ Delivered blob observed message via temp producer (key: %s) to %s [%d] at offset %v",
							string(ev.Key), *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
					}
				}
			}
		}()
		deliveryChan = tempDeliveryChan
	}

	log.Printf("Producing blob observed message for %s to Kafka topic Ingestion.Blobs: %s", blobName, string(blobObservedMsgJSON))
	err = p.Produce(kafkaMessageObserved, deliveryChan)
	if err != nil {
		return fmt.Errorf("failed to enqueue blob observed message (key: %s): %w", kafkaKeyObserved, err)
	}

	if tempProducerCreated {
		if remaining := p.Flush(15 * 1000); remaining > 0 {
			return fmt.Errorf("failed to deliver blob observed message via temp producer, %d messages remaining after flush (key: %s)", remaining, kafkaKeyObserved)
		}
		log.Printf("✅ Blob observed message (key: %s) flushed successfully using temporary producer.", kafkaKeyObserved)
	}

	return nil
}

func testKafkaConnection(brokers string) error {
	log.Printf("Testing Kafka connection to: %s", brokers)

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"message.timeout.ms": 5000, // 5 second timeout
	})
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer producer.Close()

	// Try to get metadata to test connection
	log.Printf("Attempting to get metadata...")
	start := time.Now()
	metadata, err := producer.GetMetadata(nil, false, 5000) // 5 second timeout
	duration := time.Since(start)

	if err != nil {
		return fmt.Errorf("connection failed after %v: %w", duration, err)
	}

	log.Printf("✅ Connected successfully to %d brokers in %v", len(metadata.Brokers), duration)
	for _, broker := range metadata.Brokers {
		log.Printf("  - Broker %d: %s:%d", broker.ID, broker.Host, broker.Port)
	}

	return nil
}

// Example main function to demonstrate usage
func main() {
	// Add Kafka connection test capability
	if len(os.Args) > 1 && os.Args[1] == "test-kafka" {
		brokers := "localhost:9092"
		if len(os.Args) > 2 {
			brokers = os.Args[2]
		}

		err := testKafkaConnection(brokers)
		if err != nil {
			log.Fatalf("❌ Kafka connection test failed: %v", err)
		}

		log.Println("✅ Kafka connection test passed!")
		return
	}

	// Get environment variables
	subscriptionID := os.Getenv("SUBSCRIPTION_ID")
	environment := os.Getenv("ENVIRONMENT")
	azureContainerName := os.Getenv("AZURE_STORAGE_CONTAINER_NAME")
	azureBlobName := os.Getenv("AZURE_STORAGE_BLOB_NAME")
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		kafkaTopic = "Ingestion.RawLogs" // Default topic name
	}

	var startOffset int64 = 0 // Example: start from the beginning
	// To read startOffset from env (optional):
	if offsetStr := os.Getenv("START_OFFSET"); offsetStr != "" {
		parsedOffset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err == nil {
			startOffset = parsedOffset
		} else {
			log.Printf("Warning: Could not parse START_OFFSET ('%s'), using default %d. Error: %v", offsetStr, startOffset, err)
		}
	}

	if subscriptionID == "" || environment == "" || azureContainerName == "" || azureBlobName == "" || kafkaBrokers == "" || kafkaTopic == "" {
		log.Fatalf("Error: SUBSCRIPTION_ID, ENVIRONMENT, AZURE_STORAGE_CONTAINER_NAME, AZURE_STORAGE_BLOB_NAME, KAFKA_BROKERS, and KAFKA_TOPIC environment variables must be set.")
	}

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	storageConfig, err := cfg.GetStorageAccount(subscriptionID, environment)
	if err != nil {
		log.Fatalf("Failed to get storage account for %s/%s: %v", subscriptionID, environment, err)
	}

	// Validate Kafka connectivity early to avoid wasting time on Azure operations
	log.Printf("🔍 Validating Kafka connectivity before processing...")
	err = testKafkaConnection(kafkaBrokers)
	if err != nil {
		log.Fatalf("❌ Kafka connectivity check failed: %v", err)
	}
	log.Printf("✅ Kafka connectivity validated")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	log.Printf("Starting Blob to Kafka process...")
	log.Printf("Azure Account: %s, Container: %s, Blob: %s", storageConfig.AccountName, azureContainerName, azureBlobName)
	log.Printf("Kafka Brokers: %s, Topic: %s", kafkaBrokers, kafkaTopic)
	log.Printf("Start Offset: %d", startOffset)

	err = ProcessBlobSegmentToKafka(ctx, storageConfig, azureContainerName, azureBlobName, startOffset, kafkaBrokers, kafkaTopic, subscriptionID, environment)
	if err != nil {
		log.Fatalf("🚨 Error processing blob to Kafka: %v", err)
	}

	log.Println("🎉 Blob to Kafka processing completed successfully.")
}
