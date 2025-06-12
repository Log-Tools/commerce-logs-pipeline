package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	ingestConfig "github.com/Log-Tools/commerce-logs-pipeline/ingest/internal/config"
	"github.com/Log-Tools/commerce-logs-pipeline/ingest/internal/ingestion"
	"github.com/Log-Tools/commerce-logs-pipeline/ingest/internal/service"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	// Parse command line arguments
	var (
		configFile = flag.String("config", "", "Path to configuration file")
		mode       = flag.String("mode", "", "Mode to run in: 'cli' or 'worker'")
		testKafka  = flag.Bool("test-kafka", false, "Test Kafka connection")
		brokers    = flag.String("brokers", "", "Kafka brokers for testing (optional)")
	)
	flag.Parse()

	// Handle test-kafka command
	if *testKafka {
		kafkaBrokers := "localhost:9092"
		if *brokers != "" {
			kafkaBrokers = *brokers
		}
		if err := testKafkaConnection(kafkaBrokers); err != nil {
			log.Fatalf("❌ Kafka connection test failed: %v", err)
		}
		log.Printf("✅ Kafka connection test successful")
		return
	}

	// Load configuration
	var cfg *ingestConfig.Config
	var err error

	if *configFile != "" {
		cfg, err = ingestConfig.LoadConfigFromFile(*configFile)
		if err != nil {
			log.Fatalf("❌ Failed to load configuration from file: %v", err)
		}
	} else {
		cfg, err = ingestConfig.LoadConfigFromEnv()
		if err != nil {
			log.Fatalf("❌ Failed to load configuration from environment: %v", err)
		}
	}

	// Override mode if specified
	if *mode != "" {
		cfg.Mode = *mode
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("🛑 Received signal %v, shutting down gracefully...", sig)
		cancel()
	}()

	// Run based on mode
	switch cfg.Mode {
	case "cli":
		err = runCLIMode(ctx, cfg)
	case "worker":
		err = runWorkerMode(ctx, cfg)
	default:
		log.Fatalf("❌ Unknown mode: %s. Must be 'cli' or 'worker'", cfg.Mode)
	}

	if err != nil {
		log.Fatalf("❌ Application failed: %v", err)
	}

	log.Printf("✅ Application finished successfully")
}

// runCLIMode runs the application in CLI mode for backwards compatibility
func runCLIMode(ctx context.Context, cfg *ingestConfig.Config) error {
	log.Printf("🔧 Running in CLI mode")

	// Create storage factory
	storageFactory, err := ingestion.NewAzureStorageClientFactory()
	if err != nil {
		return fmt.Errorf("failed to create storage factory: %w", err)
	}

	// Create Kafka producer optimized for single-replica guaranteed delivery
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     cfg.Kafka.Brokers,
		"acks":                                  cfg.Kafka.Producer.Acks, // "all" but with replication factor 1
		"linger.ms":                             0,                       // No linger - send immediately
		"batch.size":                            1024,                    // Very small batch size
		"compression.type":                      "none",                  // No compression for speed
		"max.in.flight.requests.per.connection": 1,                       // Single request for guaranteed ordering
		"retries":                               2147483647,              // Max retries for guaranteed delivery
		"retry.backoff.ms":                      50,                      // Faster retry backoff
		"request.timeout.ms":                    5000,                    // Very short request timeout
		"delivery.timeout.ms":                   15000,                   // 15 second delivery timeout
		"enable.idempotence":                    true,                    // Ensure exactly-once delivery
	})
	if err != nil {
		return fmt.Errorf("failed to create Kafka producer: %w", err)
	}
	defer producer.Close()

	// Create producer adapter
	producerAdapter := &kafkaProducerAdapter{producer: producer}

	// Create blob processor
	blobProcessor := ingestion.NewBlobProcessor(cfg, producerAdapter, storageFactory)

	// Create processing info from CLI config
	processingInfo := ingestion.BlobProcessingInfo{
		ContainerName: cfg.CLI.ContainerName,
		BlobName:      cfg.CLI.BlobName,
		StartOffset:   cfg.CLI.StartOffset,
		Subscription:  cfg.CLI.SubscriptionID,
		Environment:   cfg.CLI.Environment,
	}

	// Process the blob
	log.Printf("🔄 Processing blob: %s/%s from offset %d",
		processingInfo.ContainerName, processingInfo.BlobName, processingInfo.StartOffset)

	result, err := blobProcessor.ProcessBlob(ctx, processingInfo)
	if err != nil {
		return err
	}

	log.Printf("✅ Processed %d lines up to offset %d", result.LinesProcessed, result.ProcessedToOffset)
	return nil
}

// runWorkerMode runs the application in worker mode
func runWorkerMode(ctx context.Context, cfg *ingestConfig.Config) error {
	log.Printf("⚙️ Running in worker mode")

	// Create storage factory
	storageFactory, err := ingestion.NewAzureStorageClientFactory()
	if err != nil {
		return fmt.Errorf("failed to create storage factory: %w", err)
	}

	// Create Kafka producer optimized for single-replica guaranteed delivery
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     cfg.Kafka.Brokers,
		"acks":                                  cfg.Kafka.Producer.Acks, // "all" but with replication factor 1
		"linger.ms":                             0,                       // No linger - send immediately
		"batch.size":                            1024,                    // Very small batch size
		"compression.type":                      "none",                  // No compression for speed
		"max.in.flight.requests.per.connection": 1,                       // Single request for guaranteed ordering
		"retries":                               2147483647,              // Max retries for guaranteed delivery
		"retry.backoff.ms":                      50,                      // Faster retry backoff
		"request.timeout.ms":                    5000,                    // Very short request timeout
		"delivery.timeout.ms":                   15000,                   // 15 second delivery timeout
		"enable.idempotence":                    true,                    // Ensure exactly-once delivery
	})
	if err != nil {
		return fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	// Create Kafka consumer with unique group to always read from beginning
	consumerGroup := fmt.Sprintf("%s-%d", cfg.Worker.ConsumerGroup, time.Now().Unix())
	log.Printf("🔍 Consumer group: %s", consumerGroup)
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.Kafka.Brokers,
		"group.id":           consumerGroup,
		"auto.offset.reset":  cfg.Kafka.Consumer.AutoOffsetReset,
		"enable.auto.commit": cfg.Kafka.Consumer.EnableAutoCommit,
	})
	if err != nil {
		producer.Close() // Clean up producer if consumer creation fails
		return fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	// Create adapters
	producerAdapter := &kafkaProducerAdapter{producer: producer}
	consumerAdapter := &kafkaConsumerAdapter{consumer: consumer}

	// Create and start worker
	worker, err := service.NewIngestionWorker(cfg, producerAdapter, consumerAdapter, storageFactory)
	if err != nil {
		producer.Close()
		consumer.Close()
		return fmt.Errorf("failed to create ingestion worker: %w", err)
	}

	// Start worker and handle cleanup on completion
	workerErr := worker.Start(ctx)

	// Clean up resources after worker stops
	log.Printf("🧹 Cleaning up Kafka connections...")
	producer.Close()
	consumer.Close()

	return workerErr
}

// testKafkaConnection tests the connection to Kafka
func testKafkaConnection(brokers string) error {
	log.Printf("🔍 Testing Kafka connection to: %s", brokers)

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"acks":              "all",
	})
	if err != nil {
		return fmt.Errorf("failed to create test producer: %w", err)
	}
	defer producer.Close()

	// Get metadata to test connection
	metadata, err := producer.GetMetadata(nil, false, 5000)
	if err != nil {
		return fmt.Errorf("failed to get metadata: %w", err)
	}

	log.Printf("✅ Connected to Kafka cluster with %d brokers", len(metadata.Brokers))
	for _, broker := range metadata.Brokers {
		log.Printf("  - Broker %d: %s:%d", broker.ID, broker.Host, broker.Port)
	}

	return nil
}

// kafkaProducerAdapter adapts Kafka producer to our interface
type kafkaProducerAdapter struct {
	producer *kafka.Producer
}

func (a *kafkaProducerAdapter) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	return a.producer.Produce(msg, deliveryChan)
}

func (a *kafkaProducerAdapter) Events() chan kafka.Event {
	return a.producer.Events()
}

func (a *kafkaProducerAdapter) Flush(timeoutMs int) int {
	return a.producer.Flush(timeoutMs)
}

func (a *kafkaProducerAdapter) Close() {
	a.producer.Close()
}

// kafkaConsumerAdapter adapts Kafka consumer to our interface
type kafkaConsumerAdapter struct {
	consumer *kafka.Consumer
}

func (a *kafkaConsumerAdapter) Subscribe(topics []string, rebalanceCb kafka.RebalanceCb) error {
	return a.consumer.SubscribeTopics(topics, rebalanceCb)
}

func (a *kafkaConsumerAdapter) Poll(timeoutMs int) kafka.Event {
	return a.consumer.Poll(timeoutMs)
}

func (a *kafkaConsumerAdapter) Commit() ([]kafka.TopicPartition, error) {
	return a.consumer.Commit()
}

func (a *kafkaConsumerAdapter) Close() error {
	return a.consumer.Close()
}
