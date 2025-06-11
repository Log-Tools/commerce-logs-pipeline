package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Log-Tools/commerce-logs-pipeline/config"
	blobConfig "github.com/Log-Tools/commerce-logs-pipeline/pipeline/blob-monitor/internal/config"
	"github.com/Log-Tools/commerce-logs-pipeline/pipeline/blob-monitor/internal/events"
	"github.com/Log-Tools/commerce-logs-pipeline/pipeline/blob-monitor/internal/selectors"
	sharedEvents "github.com/Log-Tools/commerce-logs-pipeline/pipeline/events"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Abstracts Kafka producer operations to enable testing with mocks
type Producer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Events() chan kafka.Event
	Flush(timeoutMs int) int
	Close()
}

// Abstracts storage client creation to enable dependency injection and testing
type StorageClientFactory interface {
	CreateClients(cfg *blobConfig.Config) (map[string]*azblob.Client, error)
}

// Implements Azure storage client creation for production use
type AzureStorageClientFactory struct{}

// Creates Azure blob storage clients for all enabled environments
func (f *AzureStorageClientFactory) CreateClients(cfg *blobConfig.Config) (map[string]*azblob.Client, error) {
	storageClients := make(map[string]*azblob.Client)
	storageConfig, err := config.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load storage config: %w", err)
	}

	for _, env := range cfg.Environments {
		if !env.Enabled {
			continue
		}

		clientKey := fmt.Sprintf("%s-%s", env.Subscription, env.Environment)

		// Get storage account for this environment
		account, err := storageConfig.GetStorageAccount(env.Subscription, env.Environment)
		if err != nil {
			return nil, fmt.Errorf("failed to get storage account for %s/%s: %w",
				env.Subscription, env.Environment, err)
		}

		// Create Azure client
		cred, err := azblob.NewSharedKeyCredential(account.AccountName, account.AccessKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create credentials for %s/%s: %w",
				env.Subscription, env.Environment, err)
		}

		serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", account.AccountName)
		client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure client for %s/%s: %w",
				env.Subscription, env.Environment, err)
		}

		storageClients[clientKey] = client
	}

	return storageClients, nil
}

// Provides empty storage clients for testing scenarios
type MockStorageClientFactory struct{}

// Returns empty client map for unit testing without Azure dependencies
func (f *MockStorageClientFactory) CreateClients(cfg *blobConfig.Config) (map[string]*azblob.Client, error) {
	return make(map[string]*azblob.Client), nil
}

// Orchestrates blob monitoring across multiple environments with configurable selectors
type BlobMonitorService struct {
	config               *blobConfig.Config
	storageClients       map[string]*azblob.Client // subscription+env -> client
	kafkaProducer        Producer
	selectors            map[string]*selectors.BlobSelector
	blobClosingProcessor *BlobClosingProcessor
	blobStateProcessor   *BlobStateProcessor
	stopChannel          chan struct{}
}

// Constructs service instance from configuration file path for simple deployment scenarios
func NewBlobMonitorServiceFromFile(configPath string) (*BlobMonitorService, error) {
	// Load and validate configuration
	cfg, err := blobConfig.LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	return NewBlobMonitorServiceWithConfig(cfg)
}

// Constructs service instance from existing configuration for programmatic usage
func NewBlobMonitorServiceWithConfig(cfg *blobConfig.Config) (*BlobMonitorService, error) {
	// Initialize Kafka producer
	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers": cfg.Kafka.Brokers,
	}

	// Add producer config from YAML
	for key, value := range cfg.Kafka.ProducerConfig {
		kafkaConfig.SetKey(key, value)
	}

	producer, err := kafka.NewProducer(kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	// Use real Azure storage client factory for production
	storageFactory := &AzureStorageClientFactory{}
	return NewBlobMonitorService(cfg, producer, storageFactory)
}

// Constructs service instance with dependency injection for testing and flexibility
func NewBlobMonitorService(cfg *blobConfig.Config, producer Producer, storageFactory StorageClientFactory) (*BlobMonitorService, error) {
	// Create storage clients using injected factory
	storageClients, err := storageFactory.CreateClients(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage clients: %w", err)
	}

	// Create blob state processor
	blobStateProcessor, err := NewBlobStateProcessor(cfg, producer)
	if err != nil {
		return nil, fmt.Errorf("failed to create blob state processor: %w", err)
	}

	service := &BlobMonitorService{
		config:               cfg,
		storageClients:       storageClients,
		kafkaProducer:        producer,
		selectors:            selectors.GetBlobSelectors(),
		blobClosingProcessor: NewBlobClosingProcessor(cfg, producer),
		blobStateProcessor:   blobStateProcessor,
		stopChannel:          make(chan struct{}),
	}

	return service, nil
}

// Initiates blob monitoring workflows based on configuration requirements
func (blobMonitorService *BlobMonitorService) Start(ctx context.Context) error {
	log.Printf("🚀 Starting Blob Monitor Service")
	log.Printf("📡 Kafka: %s -> %s", blobMonitorService.config.Kafka.Brokers, blobMonitorService.config.Kafka.Topic)
	log.Printf("🌍 Monitoring %d environments", len(blobMonitorService.config.Environments))

	// Start Kafka delivery report handler
	go blobMonitorService.handleKafkaDeliveryReports()

	// Start blob closing processor in separate goroutine if enabled
	if blobMonitorService.config.Global.BlobClosingConfig.Enabled {
		log.Printf("🔐 Blob closing detection enabled (timeout: %d minutes, event-driven)",
			blobMonitorService.config.Global.BlobClosingConfig.TimeoutMinutes)
		go blobMonitorService.blobClosingProcessor.Start(ctx)
	}

	// Start blob state processor in separate goroutine
	log.Printf("🗂️ Starting blob state processor for compacted state tracking")
	go func() {
		if err := blobMonitorService.blobStateProcessor.Start(ctx); err != nil {
			log.Printf("❌ Blob state processor error: %v", err)
		}
	}()

	// Perform historical scan first
	if err := blobMonitorService.performHistoricalScan(ctx); err != nil {
		log.Printf("❌ Historical scan failed: %v", err)
		if !blobMonitorService.config.ErrorHandling.ContinueOnEnvironmentError {
			return fmt.Errorf("historical scan failed: %w", err)
		}
	}

	// Start current day monitoring if enabled
	if blobMonitorService.config.DateRange.MonitorCurrentDay {
		log.Printf("📈 Starting current day monitoring...")
		blobMonitorService.startCurrentDayMonitoring(ctx)
	}

	// Wait for stop signal
	<-blobMonitorService.stopChannel
	log.Printf("🛑 Blob monitor service stopped")
	return nil
}

// Coordinates graceful shutdown to prevent data loss and connection leaks
func (blobMonitorService *BlobMonitorService) Stop() {
	log.Printf("🔄 Stopping blob monitor service...")
	close(blobMonitorService.stopChannel)

	// Stop blob closing processor if enabled
	if blobMonitorService.config.Global.BlobClosingConfig.Enabled {
		blobMonitorService.blobClosingProcessor.Stop()
	}

	// Stop blob state processor
	blobMonitorService.blobStateProcessor.Stop()

	// Flush and close Kafka producer
	blobMonitorService.kafkaProducer.Flush(30 * 1000) // 30 second timeout
	blobMonitorService.kafkaProducer.Close()
}

// Processes historical date range to catch up on missed blobs before real-time monitoring
func (blobMonitorService *BlobMonitorService) performHistoricalScan(ctx context.Context) error {
	log.Printf("🔍 Starting historical blob scan...")

	startDate, err := blobMonitorService.config.GetStartDate()
	if err != nil {
		return fmt.Errorf("failed to get start date: %w", err)
	}

	location, err := time.LoadLocation(blobMonitorService.config.Global.Timezone)
	if err != nil {
		return fmt.Errorf("invalid timezone: %w", err)
	}

	now := time.Now().In(location)
	yesterday := now.AddDate(0, 0, -1)

	log.Printf("📅 Scanning from %s to %s",
		startDate.Format("2006-01-02"), yesterday.Format("2006-01-02"))

	// Scan each date from start to yesterday
	for date := startDate; !date.After(yesterday); date = date.AddDate(0, 0, 1) {
		dateStr := date.Format("20060102")

		log.Printf("📅 Scanning date: %s", dateStr)

		for _, env := range blobMonitorService.config.Environments {
			if !env.Enabled {
				continue
			}

			if err := blobMonitorService.scanEnvironmentForDate(ctx, env, dateStr); err != nil {
				log.Printf("❌ Failed to scan %s/%s for %s: %v",
					env.Subscription, env.Environment, dateStr, err)

				if !blobMonitorService.config.ErrorHandling.ContinueOnEnvironmentError {
					return fmt.Errorf("environment scan failed: %w", err)
				}
			}
		}
	}

	log.Printf("✅ Historical scan completed")
	return nil
}

// Launches concurrent monitoring goroutines for real-time blob detection
func (blobMonitorService *BlobMonitorService) startCurrentDayMonitoring(ctx context.Context) {
	// Start monitoring goroutines for each environment
	for _, env := range blobMonitorService.config.Environments {
		if !env.Enabled {
			continue
		}

		go blobMonitorService.monitorEnvironment(ctx, env)
	}
}

// Maintains persistent monitoring loop for a single environment with configurable intervals
func (blobMonitorService *BlobMonitorService) monitorEnvironment(ctx context.Context, env blobConfig.EnvironmentConfig) {
	pollingInterval := time.Duration(env.GetPollingInterval(blobMonitorService.config.Global.PollingInterval)) * time.Second
	ticker := time.NewTicker(pollingInterval)
	defer ticker.Stop()

	log.Printf("📡 Started monitoring %s/%s (interval: %v)", env.Subscription, env.Environment, pollingInterval)

	for {
		select {
		case <-ctx.Done():
			log.Printf("📡 Stopped monitoring %s/%s due to context cancellation", env.Subscription, env.Environment)
			return
		case <-blobMonitorService.stopChannel:
			log.Printf("📡 Stopped monitoring %s/%s due to service shutdown", env.Subscription, env.Environment)
			return
		case <-ticker.C:
			if err := blobMonitorService.performCurrentDayMonitoring(ctx, env); err != nil {
				log.Printf("❌ Current day monitoring failed for %s/%s: %v", env.Subscription, env.Environment, err)
			}
		}
	}
}

// Handles current day scanning with end-of-day overlap period to catch cross-midnight blobs
func (blobMonitorService *BlobMonitorService) performCurrentDayMonitoring(ctx context.Context, env blobConfig.EnvironmentConfig) error {
	location, err := time.LoadLocation(blobMonitorService.config.Global.Timezone)
	if err != nil {
		return fmt.Errorf("invalid timezone: %w", err)
	}

	now := time.Now().In(location)
	currentDate := now.Format("20060102")

	// Always scan current day
	if err := blobMonitorService.scanEnvironmentForDate(ctx, env, currentDate); err != nil {
		return fmt.Errorf("failed to scan current day: %w", err)
	}

	// Check if we're in EOD overlap period and scan previous day too
	inOverlap, err := blobMonitorService.config.IsInEODOverlapPeriod()
	if err != nil {
		log.Printf("⚠️ Failed to check EOD overlap period: %v", err)
	} else if inOverlap {
		previousDate := now.AddDate(0, 0, -1).Format("20060102")
		if err := blobMonitorService.scanEnvironmentForDate(ctx, env, previousDate); err != nil {
			log.Printf("⚠️ Failed to scan previous day during EOD overlap: %v", err)
		}
	}

	return nil
}

// Orchestrates blob discovery across all configured selectors for a given environment and date
func (blobMonitorService *BlobMonitorService) scanEnvironmentForDate(ctx context.Context, env blobConfig.EnvironmentConfig, date string) error {
	clientKey := fmt.Sprintf("%s-%s", env.Subscription, env.Environment)
	client, exists := blobMonitorService.storageClients[clientKey]
	if !exists {
		return fmt.Errorf("no Azure client for %s/%s", env.Subscription, env.Environment)
	}

	// Scan each configured selector
	for _, selectorName := range env.Selectors {
		selector, err := selectors.GetSelector(selectorName)
		if err != nil {
			return fmt.Errorf("failed to get selector %s: %w", selectorName, err)
		}

		if err := blobMonitorService.scanSelectorForDate(ctx, client, env, selector, date); err != nil {
			log.Printf("❌ Failed to scan selector %s for %s/%s on %s: %v",
				selectorName, env.Subscription, env.Environment, date, err)

			if !blobMonitorService.config.ErrorHandling.ContinueOnEnvironmentError {
				return err
			}
		}
	}

	return nil
}

// Executes Azure blob listing and publishes discovered blobs with precise timing metadata
func (blobMonitorService *BlobMonitorService) scanSelectorForDate(ctx context.Context, client *azblob.Client,
	env blobConfig.EnvironmentConfig, selector *selectors.BlobSelector, date string) error {

	// Capture listing start time BEFORE making the request (critical for consistency)
	listingStartTime := time.Now().UTC()

	log.Printf("🔍 Scanning %s/%s for %s on %s", env.Subscription, env.Environment, selector.Name, date)

	// List blobs with date prefix
	prefix := selector.GetDatePrefix(date)
	containerClient := client.ServiceClient().NewContainerClient(blobMonitorService.config.Storage.ContainerName)

	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	var foundBlobs []*container.BlobItem
	var totalBytes int64

	// Collect all blobs
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list blobs: %w", err)
		}

		for _, blob := range page.Segment.BlobItems {
			if selector.Predicate(*blob.Name) {
				foundBlobs = append(foundBlobs, blob)
				if blob.Properties.ContentLength != nil {
					totalBytes += *blob.Properties.ContentLength
				}
			}
		}
	}

	listingEndTime := time.Now().UTC()

	log.Printf("📦 Found %d %s blobs for %s/%s on %s (%.2f MB)",
		len(foundBlobs), selector.Name, env.Subscription, env.Environment, date,
		float64(totalBytes)/(1024*1024))

	// Publish BlobObserved events for each found blob
	for _, blob := range foundBlobs {
		event := events.BlobObservedEvent{
			Subscription:     env.Subscription,
			Environment:      env.Environment,
			BlobName:         *blob.Name,
			ObservationDate:  time.Now().UTC(),
			SizeInBytes:      *blob.Properties.ContentLength,
			LastModifiedDate: *blob.Properties.LastModified,
			ServiceSelector:  selector.Name,
		}

		// Add creation time if available
		if blob.Properties.CreationTime != nil {
			event.CreationDate = *blob.Properties.CreationTime
		}

		if err := blobMonitorService.publishBlobObservedEvent(event); err != nil {
			log.Printf("❌ Failed to publish BlobObserved event for %s: %v", *blob.Name, err)
		}

		// Track blob for closing detection if enabled
		if blobMonitorService.config.Global.BlobClosingConfig.Enabled {
			blobMonitorService.blobClosingProcessor.ProcessBlobObserved(event)
		}
	}

	// Publish BlobsListed completion event
	completionEvent := events.BlobsListedEvent{
		Subscription:     env.Subscription,
		Environment:      env.Environment,
		ServiceSelector:  selector.Name,
		Date:             date,
		ListingStartTime: listingStartTime,
		ListingEndTime:   listingEndTime,
		BlobCount:        len(foundBlobs),
		TotalBytes:       totalBytes,
	}

	if err := blobMonitorService.publishBlobsListedEvent(completionEvent); err != nil {
		return fmt.Errorf("failed to publish BlobsListed event: %w", err)
	}

	return nil
}

// Serializes and publishes individual blob discovery events to enable downstream processing
func (blobMonitorService *BlobMonitorService) publishBlobObservedEvent(event events.BlobObservedEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal BlobObserved event: %w", err)
	}

	key := sharedEvents.GenerateBlobEventKey(event.Subscription, event.Environment, "observed", event.BlobName)

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &blobMonitorService.config.Kafka.Topic,
		},
		Key:   []byte(key),
		Value: eventJSON,
	}

	return blobMonitorService.kafkaProducer.Produce(message, nil)
}

// Publishes scan completion events with aggregated statistics for monitoring and debugging
func (blobMonitorService *BlobMonitorService) publishBlobsListedEvent(event events.BlobsListedEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal BlobsListed event: %w", err)
	}

	key := sharedEvents.GenerateBlobEventKey(event.Subscription, event.Environment, "listed", fmt.Sprintf("%s-%s", event.ServiceSelector, event.Date))

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &blobMonitorService.config.Kafka.Topic,
		},
		Key:   []byte(key),
		Value: eventJSON,
	}

	return blobMonitorService.kafkaProducer.Produce(message, nil)
}

// Monitors Kafka delivery confirmations to detect message publishing failures
func (blobMonitorService *BlobMonitorService) handleKafkaDeliveryReports() {
	for e := range blobMonitorService.kafkaProducer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("❌ Kafka delivery failed for key %s: %v",
					string(ev.Key), ev.TopicPartition.Error)
			} else if blobMonitorService.config.Monitoring.LogLevel == "debug" {
				log.Printf("✅ Kafka delivery successful for key %s to %s[%d]@%v",
					string(ev.Key), *ev.TopicPartition.Topic,
					ev.TopicPartition.Partition, ev.TopicPartition.Offset)
			}
		case kafka.Error:
			log.Printf("❌ Kafka producer error: %v", ev)
		}
	}
}

// Test helper methods for integration testing

// PublishBlobObservedEvent exposes blob event publishing for integration tests
func (blobMonitorService *BlobMonitorService) PublishBlobObservedEvent(event events.BlobObservedEvent) error {
	return blobMonitorService.publishBlobObservedEvent(event)
}

// PublishBlobsListedEvent exposes completion event publishing for integration tests
func (blobMonitorService *BlobMonitorService) PublishBlobsListedEvent(event events.BlobsListedEvent) error {
	return blobMonitorService.publishBlobsListedEvent(event)
}

// PublishBlobClosedEvent exposes blob closed event publishing for integration tests
func (blobMonitorService *BlobMonitorService) PublishBlobClosedEvent(event events.BlobClosedEvent) error {
	return blobMonitorService.blobClosingProcessor.publishBlobClosedEvent(event)
}
