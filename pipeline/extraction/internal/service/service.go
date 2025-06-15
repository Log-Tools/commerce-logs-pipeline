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

// Service represents the main extraction service
type Service struct {
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

// Assembles an extraction service with all dependencies injected for testability
func NewService(
	cfg *config.Config,
	consumer Consumer,
	producer Producer,
	extractor Extractor,
	metricsCollector MetricsCollector,
) *Service {
	return &Service{
		config:           cfg,
		consumer:         consumer,
		producer:         producer,
		extractor:        extractor,
		metricsCollector: metricsCollector,
	}
}

// Loads configuration and initializes service with production dependencies
func NewServiceFromFile(configPath string) (*Service, error) {
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	return NewServiceWithConfig(cfg)
}

// Builds production Kafka clients and extractor components from configuration
func NewServiceWithConfig(cfg *config.Config) (*Service, error) {
	kafkaFactory := &DefaultKafkaClientFactory{}
	extractorFactory := &DefaultExtractorFactory{}
	metricsFactory := &DefaultMetricsFactory{}

	consumer, err := kafkaFactory.CreateConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.ConsumerGroup,
		cfg.Kafka.ConsumerConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
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

	return NewService(cfg, consumer, producer, extractor, metricsCollector), nil
}

// Run starts the extraction service processing loop
func (s *Service) Run(ctx context.Context) error {
	log.Printf("Starting extraction service with config: brokers=%s, input_topic=%s, output_topic=%s, workers=%d",
		s.config.Kafka.Brokers, s.config.Kafka.InputTopic, s.config.Kafka.OutputTopic, s.config.Processing.MaxConcurrency)

	// Subscribe to input topic
	err := s.consumer.Subscribe([]string{s.config.Kafka.InputTopic}, nil)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %w", s.config.Kafka.InputTopic, err)
	}

	// Start producer event handler
	go s.handleProducerEvents()

	// Create message channel for worker coordination with larger buffer for smooth flow
	// Buffer size = workers * 10 to handle bursts while maintaining backpressure
	bufferSize := s.config.Processing.MaxConcurrency * 10
	messageChannel := make(chan *kafka.Message, bufferSize)
	errorChannel := make(chan error, s.config.Processing.MaxConcurrency)

	// Start worker goroutines for concurrent processing
	for i := 0; i < s.config.Processing.MaxConcurrency; i++ {
		workerID := i + 1
		go s.messageWorker(ctx, workerID, messageChannel, errorChannel)
	}

	log.Printf("Extraction service started with %d workers (buffer: %d), waiting for messages...",
		s.config.Processing.MaxConcurrency, bufferSize)

	// Main loop - read messages and distribute to workers
	for {
		select {
		case <-ctx.Done():
			log.Println("Context cancelled, stopping extraction service")
			close(messageChannel) // Signal workers to stop
			return nil
		case err := <-errorChannel:
			// Log worker errors but continue processing
			log.Printf("Worker error: %v", err)
		default:
			// Read message with timeout
			msg, err := s.consumer.ReadMessage(1000) // 1 second timeout
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue // Timeout is expected, not an error
				}
				log.Printf("Failed to read message: %v", err)
				continue
			}

			// Send message to available worker (with backpressure)
			select {
			case messageChannel <- msg:
				// Message sent to worker successfully
			case <-ctx.Done():
				return nil
			}
			// Removed 'default' case - we should block if workers are busy, not drop messages!
		}
	}
}

// messageWorker processes messages concurrently
func (s *Service) messageWorker(ctx context.Context, workerID int, messageChannel <-chan *kafka.Message, errorChannel chan<- error) {
	log.Printf("Worker %d started", workerID)
	defer log.Printf("Worker %d stopped", workerID)

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messageChannel:
			if !ok {
				// Channel closed, worker should exit
				return
			}

			// Process the message
			if err := s.processMessageConcurrent(ctx, workerID, msg); err != nil {
				// Send error to error channel (non-blocking)
				select {
				case errorChannel <- fmt.Errorf("worker %d: %w", workerID, err):
				default:
					// Error channel full, log directly
					log.Printf("Worker %d error (channel full): %v", workerID, err)
				}
			}
		}
	}
}

// processMessageConcurrent handles a single message (thread-safe version)
func (s *Service) processMessageConcurrent(ctx context.Context, workerID int, msg *kafka.Message) error {
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
		log.Printf("Statistics: processed=%d, extracted=%d, validation_errors=%d, extraction_errors=%d (worker %d)",
			processedCount, successCount, validationErrors, extractionErrors, workerID)
	}

	// Extract message metadata for source information
	source := s.extractSourceFromMessage(msg)

	// Process the message
	rawLine := string(msg.Value)
	extractedLog, err := s.extractor.ExtractLog(rawLine, source)
	if err != nil {
		s.metricsCollector.IncrementExtractionErrors()
		atomic.AddInt64(&s.extractionErrors, 1)
		if s.config.Processing.LogParseErrors {
			log.Printf("Worker %d: Failed to extract log: %v", workerID, err)
		}

		// Send extraction error event if not skipping invalid logs
		if !s.config.Processing.SkipInvalidLogs {
			return s.sendExtractionError(rawLine, err, "parse_error", source, msg)
		}

		// Commit message and continue if skipping invalid logs
		return s.consumer.CommitMessage(msg)
	}

	// If extractedLog is nil (but no error), it means the message should be silently skipped
	// This happens for empty log messages which are common in container logs
	if extractedLog == nil {
		atomic.AddInt64(&s.successCount, 1) // Count as successful (not an error)
		return s.consumer.CommitMessage(msg)
	}

	// Additional check for nil pointers wrapped in interfaces (Go gotcha)
	// This can happen when a nil *events.ApplicationLog is returned as interface{}
	switch log := extractedLog.(type) {
	case *events.HTTPRequestLog:
		if log == nil {
			atomic.AddInt64(&s.successCount, 1) // Count as successful (not an error)
			return s.consumer.CommitMessage(msg)
		}
	case *events.ApplicationLog:
		if log == nil {
			atomic.AddInt64(&s.successCount, 1) // Count as successful (not an error)
			return s.consumer.CommitMessage(msg)
		}
	}

	// Validate extracted log if validation is enabled
	if s.config.Processing.EnableValidation {
		if err := s.extractor.ValidateExtractedLog(extractedLog); err != nil {
			s.metricsCollector.IncrementValidationErrors()
			atomic.AddInt64(&s.validationErrors, 1)
			if s.config.Processing.LogParseErrors {
				log.Printf("Worker %d: Validation failed for log: %v", workerID, err)
			}

			if !s.config.Processing.SkipInvalidLogs {
				return s.sendExtractionError(rawLine, err, "validation_error", source, msg)
			}

			return s.consumer.CommitMessage(msg)
		}
	}

	// Send extracted log to output topic
	if err := s.sendExtractedLog(extractedLog, msg); err != nil {
		return fmt.Errorf("failed to send extracted log: %w", err)
	}

	s.metricsCollector.IncrementMessagesExtracted()
	atomic.AddInt64(&s.successCount, 1)

	// Commit the message
	return s.consumer.CommitMessage(msg)
}

// extractSourceFromMessage extracts source metadata from Kafka message
func (s *Service) extractSourceFromMessage(msg *kafka.Message) events.LogSource {
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
		log.Printf("DEBUG [msg %d]: Missing headers - available headers:", currentCount)
		for _, header := range msg.Headers {
			log.Printf("  %s: %s", header.Key, string(header.Value))
		}
	}

	return source
}

// sendExtractedLog sends an extracted log to the output topic
func (s *Service) sendExtractedLog(extractedLog interface{}, originalMsg *kafka.Message) error {
	data, err := json.Marshal(extractedLog)
	if err != nil {
		return fmt.Errorf("failed to marshal extracted log: %w", err)
	}

	// Extract blob name from key and calculate partition using same FNV algorithm as ingestion
	blobName := s.extractBlobNameFromKey(string(originalMsg.Key))
	partition := s.partitionForBlob(blobName)

	// Get source information from original message headers
	source := s.extractSourceFromMessage(originalMsg)

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &s.config.Kafka.OutputTopic,
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
		log.Printf("ERROR: Failed to produce to partition %d for blob '%s' (key: %s): %v", partition, blobName, string(originalMsg.Key), err)
	}
	return err
}

// sendExtractionError sends an extraction error event
func (s *Service) sendExtractionError(rawLine string, err error, errorType string, source events.LogSource, originalMsg *kafka.Message) error {
	extractionError := &events.ExtractionError{
		RawLine:   rawLine,
		Error:     err.Error(),
		ErrorType: errorType,
		Timestamp: time.Now().UnixNano(),
		Source:    source,
	}

	data, jsonErr := json.Marshal(extractionError)
	if jsonErr != nil {
		return fmt.Errorf("failed to marshal extraction error: %w", jsonErr)
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
			{Key: "event_type", Value: []byte("extraction_error")},
			{Key: "error_type", Value: []byte(errorType)},
			{Key: "service", Value: []byte(source.Service)},
			{Key: "environment", Value: []byte(source.Environment)},
			{Key: "subscription", Value: []byte(source.Subscription)},
		},
	}

	produceErr := s.producer.Produce(msg, nil)
	if produceErr != nil {
		log.Printf("ERROR: Failed to produce error to partition %d for blob '%s' (key: %s): %v", partition, blobName, string(originalMsg.Key), produceErr)
	}
	return produceErr
}

// handleProducerEvents handles Kafka producer events
func (s *Service) handleProducerEvents() {
	for event := range s.producer.Events() {
		switch e := event.(type) {
		case *kafka.Message:
			if e.TopicPartition.Error != nil {
				log.Printf("Failed to deliver message: %v", e.TopicPartition.Error)
			}
		case kafka.Error:
			log.Printf("Producer error: %v", e)
		}
	}
}

// Close gracefully shuts down the service
func (s *Service) Close() {
	log.Println("Shutting down extraction service...")

	if s.producer != nil {
		s.producer.Flush(s.config.Kafka.FlushTimeoutMs)
		s.producer.Close()
	}

	if s.consumer != nil {
		s.consumer.Close()
	}

	log.Println("Extraction service shutdown complete")
}

// partitionForBlob calculates the partition for a given blob name using the same FNV algorithm as ingestion
func (s *Service) partitionForBlob(blobName string) int32 {
	h := fnv.New32a()
	h.Write([]byte(blobName))
	// Use 12 partitions as configured in kafka_topics.yaml for Raw.ApplicationLogs and Extracted.Application
	return int32(h.Sum32() % 12)
}

// partitionForErrorTopic calculates the partition for error messages
func (s *Service) partitionForErrorTopic(blobName string) int32 {
	h := fnv.New32a()
	h.Write([]byte(blobName))
	// Use 3 partitions as configured in kafka_topics.yaml for Extraction.Errors
	return int32(h.Sum32() % 3)
}

// extractBlobNameFromKey extracts the blob name from the message key
func (s *Service) extractBlobNameFromKey(key string) string {
	// Key format: {subscription}:{environment}:{eventType}:{blobName}
	parts := strings.Split(key, ":")
	if len(parts) >= 4 {
		return parts[3] // Return the blob name part
	}
	return key // Fallback to using the entire key if format is unexpected
}
