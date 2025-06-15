package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"pipeline/events"
	"pipeline/extraction/internal/config"

	"hash/fnv"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ProxyService represents the proxy extraction service for Raw.ProxyLogs
type ProxyService struct {
	config           *config.Config
	consumer         Consumer
	producer         Producer
	extractor        Extractor
	metricsCollector MetricsCollector
	processedCount   int64 // Total messages processed
	successCount     int64 // Successfully extracted messages
	validationErrors int64 // Validation failures
	extractionErrors int64 // Extraction failures
}

// NewProxyService creates a new proxy extraction service with all dependencies injected
func NewProxyService(
	cfg *config.Config,
	consumer Consumer,
	producer Producer,
	extractor Extractor,
	metricsCollector MetricsCollector,
) *ProxyService {
	return &ProxyService{
		config:           cfg,
		consumer:         consumer,
		producer:         producer,
		extractor:        extractor,
		metricsCollector: metricsCollector,
	}
}

// NewProxyServiceWithConfig builds production Kafka clients and extractor components for proxy logs
func NewProxyServiceWithConfig(cfg *config.Config) (*ProxyService, error) {
	kafkaFactory := &DefaultKafkaClientFactory{}
	extractorFactory := &DefaultExtractorFactory{}
	metricsFactory := &DefaultMetricsFactory{}

	consumer, err := kafkaFactory.CreateConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.ProxyConsumerGroup,
		cfg.Kafka.ConsumerConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create proxy consumer: %w", err)
	}

	producer, err := kafkaFactory.CreateProducer(
		cfg.Kafka.Brokers,
		cfg.Kafka.ProducerConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	extractor := extractorFactory.CreateExtractor()
	metricsCollector := metricsFactory.CreateMetricsCollector()

	return NewProxyService(cfg, consumer, producer, extractor, metricsCollector), nil
}

// Run starts the proxy extraction service processing loop
func (s *ProxyService) Run(ctx context.Context) error {
	log.Printf("Starting proxy extraction service with config: brokers=%s, input_topic=%s, output_topic=%s, workers=%d",
		s.config.Kafka.Brokers, s.config.Kafka.ProxyTopic, s.config.Kafka.ProxyOutputTopic, s.config.Processing.ProxyMaxConcurrency)

	// Subscribe to proxy topic
	err := s.consumer.Subscribe([]string{s.config.Kafka.ProxyTopic}, nil)
	if err != nil {
		return fmt.Errorf("failed to subscribe to proxy topic %s: %w", s.config.Kafka.ProxyTopic, err)
	}

	// Start producer event handler
	go s.handleProducerEvents()

	// Create message channel for worker coordination with larger buffer for smooth flow
	// Buffer size = workers * 10 to handle bursts while maintaining backpressure
	bufferSize := s.config.Processing.ProxyMaxConcurrency * 10
	messageChannel := make(chan *kafka.Message, bufferSize)
	errorChannel := make(chan error, s.config.Processing.ProxyMaxConcurrency)

	// Start worker goroutines for concurrent processing
	for i := 0; i < s.config.Processing.ProxyMaxConcurrency; i++ {
		workerID := i + 1
		go s.proxyWorker(ctx, workerID, messageChannel, errorChannel)
	}

	log.Printf("Proxy extraction service started with %d workers (buffer: %d), waiting for messages...",
		s.config.Processing.ProxyMaxConcurrency, bufferSize)

	// Main loop - read messages and distribute to workers
	for {
		select {
		case <-ctx.Done():
			log.Println("Context cancelled, stopping proxy extraction service")
			close(messageChannel) // Signal workers to stop
			return nil
		case err := <-errorChannel:
			// Log worker errors but continue processing
			log.Printf("Proxy worker error: %v", err)
		default:
			// Read message with timeout
			msg, err := s.consumer.ReadMessage(1000) // 1 second timeout
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue // Timeout is expected, not an error
				}
				log.Printf("Failed to read proxy message: %v", err)
				continue
			}

			// Send message to available worker (with backpressure)
			select {
			case messageChannel <- msg:
				// Message sent to worker successfully
			case <-ctx.Done():
				return nil
			}
			// Note: we block if workers are busy, ensuring no message dropping
		}
	}
}

// proxyWorker processes proxy messages concurrently
func (s *ProxyService) proxyWorker(ctx context.Context, workerID int, messageChannel <-chan *kafka.Message, errorChannel chan<- error) {
	log.Printf("Proxy worker %d started", workerID)
	defer log.Printf("Proxy worker %d stopped", workerID)

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messageChannel:
			if !ok {
				// Channel closed, worker should exit
				return
			}

			// Process the proxy message
			if err := s.processProxyMessageConcurrent(ctx, workerID, msg); err != nil {
				// Send error to error channel (non-blocking)
				select {
				case errorChannel <- fmt.Errorf("proxy worker %d: %w", workerID, err):
				default:
					// Error channel full, log directly
					log.Printf("Proxy worker %d error (channel full): %v", workerID, err)
				}
			}
		}
	}
}

// processProxyMessageConcurrent handles a single proxy message (thread-safe version)
func (s *ProxyService) processProxyMessageConcurrent(ctx context.Context, workerID int, msg *kafka.Message) error {
	startTime := time.Now()
	defer func() {
		s.metricsCollector.RecordProcessingLatency(time.Since(startTime).Milliseconds())
	}()

	// Thread-safe increment of processed count
	s.metricsCollector.IncrementMessagesProcessed()
	processedCount := atomic.AddInt64(&s.processedCount, 1)

	// Log processing statistics every 1000 messages (from any worker)
	if processedCount%1000 == 0 {
		successCount := atomic.LoadInt64(&s.successCount)
		validationErrors := atomic.LoadInt64(&s.validationErrors)
		extractionErrors := atomic.LoadInt64(&s.extractionErrors)
		log.Printf("Proxy Statistics: processed=%d, extracted=%d, validation_errors=%d, extraction_errors=%d (worker %d)",
			processedCount, successCount, validationErrors, extractionErrors, workerID)
	}

	// Extract message metadata for source information
	source := s.extractSourceFromMessage(msg)

	// Process the proxy message using dedicated proxy extraction
	rawLine := string(msg.Value)
	extractedLog, err := s.extractor.ExtractProxyLog(rawLine, source)
	if err != nil {
		s.metricsCollector.IncrementExtractionErrors()
		atomic.AddInt64(&s.extractionErrors, 1)
		if s.config.Processing.LogParseErrors {
			log.Printf("Proxy worker %d: Failed to extract proxy log: %v", workerID, err)
		}

		// Send extraction error event if not skipping invalid logs
		if !s.config.Processing.SkipInvalidLogs {
			if err := s.sendExtractionError(rawLine, err, "proxy_parse_error", source, msg); err != nil {
				return fmt.Errorf("failed to send extraction error: %w", err)
			}
		}

		// Commit message and continue if skipping invalid logs
		return s.consumer.CommitMessage(msg)
	}

	// If extractedLog is nil (but no error), it means the message should be silently skipped
	if extractedLog == nil {
		atomic.AddInt64(&s.successCount, 1) // Count as successful (not an error)
		return s.consumer.CommitMessage(msg)
	}

	// Validate extracted log if validation is enabled
	if s.config.Processing.EnableValidation {
		if err := s.extractor.ValidateExtractedLog(extractedLog); err != nil {
			s.metricsCollector.IncrementValidationErrors()
			atomic.AddInt64(&s.validationErrors, 1)
			if s.config.Processing.LogParseErrors {
				log.Printf("Proxy worker %d: Validation failed for proxy log: %v", workerID, err)
			}

			if !s.config.Processing.SkipInvalidLogs {
				if err := s.sendExtractionError(rawLine, err, "proxy_validation_error", source, msg); err != nil {
					return fmt.Errorf("failed to send validation error: %w", err)
				}
			}

			return s.consumer.CommitMessage(msg)
		}
	}

	// Send ALL extracted logs from Raw.ProxyLogs to Extracted.Proxy topic
	if err := s.sendExtractedLog(extractedLog, msg); err != nil {
		return fmt.Errorf("failed to send extracted log: %w", err)
	}

	s.metricsCollector.IncrementMessagesExtracted()
	atomic.AddInt64(&s.successCount, 1)

	// Commit the message ONLY after successful delivery confirmation
	return s.consumer.CommitMessage(msg)
}

// extractSourceFromMessage extracts source metadata from Kafka message
func (s *ProxyService) extractSourceFromMessage(msg *kafka.Message) events.LogSource {
	source := events.LogSource{}

	// Extract metadata from Kafka headers
	for _, header := range msg.Headers {
		switch header.Key {
		case "service":
			source.Service = string(header.Value)
		case "environment":
			source.Environment = string(header.Value)
		case "subscription":
			source.Subscription = string(header.Value)
		}
	}

	// Log missing headers for debugging (only for first few messages, thread-safe)
	currentCount := atomic.LoadInt64(&s.processedCount)
	if currentCount <= 3 && (source.Environment == "" || source.Subscription == "") {
		log.Printf("DEBUG [proxy msg %d]: Missing headers - available headers:", currentCount)
		for _, header := range msg.Headers {
			log.Printf("  %s: %s", header.Key, string(header.Value))
		}
	}

	return source
}

// sendExtractedLog sends any extracted log (ProxyLog or ApplicationLog) to Extracted.Proxy topic
func (s *ProxyService) sendExtractedLog(extractedLog interface{}, originalMsg *kafka.Message) error {
	data, err := json.Marshal(extractedLog)
	if err != nil {
		return fmt.Errorf("failed to marshal extracted log: %w", err)
	}

	// Extract blob name from key and calculate partition using same FNV algorithm as ingestion
	blobName := s.extractBlobNameFromKey(string(originalMsg.Key))
	partition := s.partitionForBlob(blobName)

	// Get source information from original message headers
	source := s.extractSourceFromMessage(originalMsg)

	// Send ALL proxy logs to Extracted.Proxy topic (regardless of type)
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &s.config.Kafka.ProxyOutputTopic,
			Partition: partition, // Use exact same FNV hash partitioning as ingestion
		},
		Key:   originalMsg.Key, // Preserve the original message key for consistency
		Value: data,
		Headers: []kafka.Header{
			{Key: "service", Value: []byte(source.Service)},
			{Key: "environment", Value: []byte(source.Environment)},
			{Key: "subscription", Value: []byte(source.Subscription)},
		},
	}

	err = s.producer.Produce(msg, nil)
	if err != nil {
		log.Printf("ERROR: Failed to produce log to partition %d for blob '%s' (key: %s): %v", partition, blobName, string(originalMsg.Key), err)
	}
	return err
}

// sendExtractedProxyLog sends an extracted proxy log to the proxy output topic (async, no confirmation)
func (s *ProxyService) sendExtractedProxyLog(extractedLog *events.ProxyLog, originalMsg *kafka.Message) error {
	data, err := json.Marshal(extractedLog)
	if err != nil {
		return fmt.Errorf("failed to marshal extracted proxy log: %w", err)
	}

	// Extract blob name from key and calculate partition using same FNV algorithm as ingestion
	blobName := s.extractBlobNameFromKey(string(originalMsg.Key))
	partition := s.partitionForBlob(blobName)

	// Get source information from original message headers
	source := s.extractSourceFromMessage(originalMsg)

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &s.config.Kafka.ProxyOutputTopic,
			Partition: partition, // Use exact same FNV hash partitioning as ingestion
		},
		Key:   originalMsg.Key, // Preserve the original message key for consistency
		Value: data,
		Headers: []kafka.Header{
			{Key: "service", Value: []byte(source.Service)},
			{Key: "environment", Value: []byte(source.Environment)},
			{Key: "subscription", Value: []byte(source.Subscription)},
		},
	}

	err = s.producer.Produce(msg, nil)
	if err != nil {
		log.Printf("ERROR: Failed to produce proxy log to partition %d for blob '%s' (key: %s): %v", partition, blobName, string(originalMsg.Key), err)
	}
	return err
}

// sendExtractedProxyLogWithConfirmation sends an extracted proxy log and waits for delivery confirmation
func (s *ProxyService) sendExtractedProxyLogWithConfirmation(extractedLog *events.ProxyLog, originalMsg *kafka.Message) error {
	data, err := json.Marshal(extractedLog)
	if err != nil {
		return fmt.Errorf("failed to marshal extracted proxy log: %w", err)
	}

	// Extract blob name from key and calculate partition using same FNV algorithm as ingestion
	blobName := s.extractBlobNameFromKey(string(originalMsg.Key))
	partition := s.partitionForBlob(blobName)

	// Get source information from original message headers
	source := s.extractSourceFromMessage(originalMsg)

	// Create delivery channel for confirmation
	deliveryChannel := make(chan kafka.Event, 1)

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &s.config.Kafka.ProxyOutputTopic,
			Partition: partition, // Use exact same FNV hash partitioning as ingestion
		},
		Key:   originalMsg.Key, // Preserve the original message key for consistency
		Value: data,
		Headers: []kafka.Header{
			{Key: "service", Value: []byte(source.Service)},
			{Key: "environment", Value: []byte(source.Environment)},
			{Key: "subscription", Value: []byte(source.Subscription)},
		},
	}

	// Produce with delivery channel
	err = s.producer.Produce(msg, deliveryChannel)
	if err != nil {
		log.Printf("ERROR: Failed to produce proxy log to partition %d for blob '%s' (key: %s): %v", partition, blobName, string(originalMsg.Key), err)
		close(deliveryChannel)
		return err
	}

	// Wait for delivery confirmation with timeout
	select {
	case event := <-deliveryChannel:
		close(deliveryChannel)
		if deliveryMsg, ok := event.(*kafka.Message); ok {
			if deliveryMsg.TopicPartition.Error != nil {
				log.Printf("ERROR: Failed to deliver proxy message to partition %d for blob '%s' (key: %s): %v", partition, blobName, string(originalMsg.Key), deliveryMsg.TopicPartition.Error)
				return deliveryMsg.TopicPartition.Error
			}
			// Success - message was delivered
			return nil
		}
		return fmt.Errorf("unexpected delivery event type: %T", event)
	case <-time.After(30 * time.Second):
		close(deliveryChannel)
		return fmt.Errorf("timeout waiting for delivery confirmation after 30s")
	}
}

// sendExtractionError sends a proxy extraction error event
func (s *ProxyService) sendExtractionError(rawLine string, err error, errorType string, source events.LogSource, originalMsg *kafka.Message) error {
	extractionError := &events.ExtractionError{
		RawLine:   rawLine,
		Error:     err.Error(),
		ErrorType: errorType,
		Timestamp: time.Now().UnixNano(),
		Source:    source,
	}

	data, jsonErr := json.Marshal(extractionError)
	if jsonErr != nil {
		return fmt.Errorf("failed to marshal proxy extraction error: %w", jsonErr)
	}

	// Extract blob name from key and calculate partition using same FNV algorithm as ingestion
	blobName := s.extractBlobNameFromKey(string(originalMsg.Key))
	partition := s.partitionForErrorTopic(blobName)

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &s.config.Kafka.ErrorTopic,
			Partition: partition, // Use exact same FNV hash partitioning as ingestion
		},
		Key:   originalMsg.Key, // Preserve the original message key for consistency
		Value: data,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte("proxy_extraction_error")},
			{Key: "error_type", Value: []byte(errorType)},
			{Key: "service", Value: []byte(source.Service)},
			{Key: "environment", Value: []byte(source.Environment)},
			{Key: "subscription", Value: []byte(source.Subscription)},
		},
	}

	produceErr := s.producer.Produce(msg, nil)
	if produceErr != nil {
		log.Printf("ERROR: Failed to produce proxy error to partition %d for blob '%s' (key: %s): %v", partition, blobName, string(originalMsg.Key), produceErr)
	}
	return produceErr
}

// handleProducerEvents handles Kafka producer events
func (s *ProxyService) handleProducerEvents() {
	for event := range s.producer.Events() {
		switch e := event.(type) {
		case *kafka.Message:
			if e.TopicPartition.Error != nil {
				log.Printf("Failed to deliver proxy message: %v", e.TopicPartition.Error)
			}
		case kafka.Error:
			log.Printf("Proxy producer error: %v", e)
		}
	}
}

// Close gracefully shuts down the proxy service
func (s *ProxyService) Close() {
	log.Println("Shutting down proxy extraction service...")

	if s.producer != nil {
		s.producer.Flush(s.config.Kafka.FlushTimeoutMs)
		s.producer.Close()
	}

	if s.consumer != nil {
		s.consumer.Close()
	}

	log.Println("Proxy extraction service shutdown complete")
}

// partitionForBlob calculates the partition for a given blob name using the same FNV algorithm as ingestion
func (s *ProxyService) partitionForBlob(blobName string) int32 {
	h := fnv.New32a()
	h.Write([]byte(blobName))
	// Use 12 partitions as configured in kafka_topics.yaml for Raw.ProxyLogs and Extracted.Proxy
	return int32(h.Sum32() % 12)
}

// partitionForErrorTopic calculates the partition for error messages
func (s *ProxyService) partitionForErrorTopic(blobName string) int32 {
	h := fnv.New32a()
	h.Write([]byte(blobName))
	// Use 3 partitions as configured in kafka_topics.yaml for Extraction.Errors
	return int32(h.Sum32() % 3)
}

// extractBlobNameFromKey extracts the blob name from the message key
func (s *ProxyService) extractBlobNameFromKey(key string) string {
	// Key format: {subscription}:{environment}:{eventType}:{blobName}
	parts := strings.Split(key, ":")
	if len(parts) >= 4 {
		return parts[3] // Return the blob name part
	}
	return key // Fallback to using the entire key if format is unexpected
}
