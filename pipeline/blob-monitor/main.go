package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Log-Tools/commerce-logs-pipeline/config"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// BlobMonitorService manages the blob monitoring and event publishing
type BlobMonitorService struct {
	config         *Config
	storageClients map[string]*azblob.Client // subscription+env -> client
	kafkaProducer  *kafka.Producer
	selectors      map[string]*BlobSelector
	mu             sync.RWMutex
	stopChannel    chan struct{}
}

// NewBlobMonitorService creates a new blob monitoring service instance
func NewBlobMonitorService(configPath string) (*BlobMonitorService, error) {
	// Load and validate configuration
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

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

	// Initialize storage clients for each environment
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

	service := &BlobMonitorService{
		config:         cfg,
		storageClients: storageClients,
		kafkaProducer:  producer,
		selectors:      GetBlobSelectors(),
		stopChannel:    make(chan struct{}),
	}

	return service, nil
}

// Start begins the blob monitoring service
func (blobMonitorService *BlobMonitorService) Start(ctx context.Context) error {
	log.Printf("🚀 Starting Blob Monitor Service")
	log.Printf("📡 Kafka: %s -> %s", blobMonitorService.config.Kafka.Brokers, blobMonitorService.config.Kafka.Topic)
	log.Printf("🌍 Monitoring %d environments", len(blobMonitorService.config.Environments))

	// Start Kafka delivery report handler
	go blobMonitorService.handleKafkaDeliveryReports()

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

// Stop gracefully shuts down the service
func (s *BlobMonitorService) Stop() {
	log.Printf("🔄 Stopping blob monitor service...")
	close(s.stopChannel)

	// Flush and close Kafka producer
	s.kafkaProducer.Flush(30 * 1000) // 30 second timeout
	s.kafkaProducer.Close()
}

// performHistoricalScan scans for blobs from the configured start date to yesterday
func (s *BlobMonitorService) performHistoricalScan(ctx context.Context) error {
	log.Printf("🔍 Starting historical blob scan...")

	startDate, err := s.config.GetStartDate()
	if err != nil {
		return fmt.Errorf("failed to get start date: %w", err)
	}

	location, err := time.LoadLocation(s.config.Global.Timezone)
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

		for _, env := range s.config.Environments {
			if !env.Enabled {
				continue
			}

			if err := s.scanEnvironmentForDate(ctx, env, dateStr); err != nil {
				log.Printf("❌ Failed to scan %s/%s for %s: %v",
					env.Subscription, env.Environment, dateStr, err)

				if !s.config.ErrorHandling.ContinueOnEnvironmentError {
					return fmt.Errorf("environment scan failed: %w", err)
				}
			}
		}
	}

	log.Printf("✅ Historical scan completed")
	return nil
}

// startCurrentDayMonitoring begins continuous monitoring of the current day
func (s *BlobMonitorService) startCurrentDayMonitoring(ctx context.Context) {
	// Start monitoring goroutines for each environment
	for _, env := range s.config.Environments {
		if !env.Enabled {
			continue
		}

		go s.monitorEnvironment(ctx, env)
	}
}

// monitorEnvironment continuously monitors a specific environment
func (s *BlobMonitorService) monitorEnvironment(ctx context.Context, env EnvironmentConfig) {
	interval := time.Duration(env.GetPollingInterval(s.config.Global.PollingInterval)) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("📡 Started monitoring %s/%s (interval: %v)",
		env.Subscription, env.Environment, interval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopChannel:
			return
		case <-ticker.C:
			if err := s.performCurrentDayMonitoring(ctx, env); err != nil {
				log.Printf("❌ Current day monitoring failed for %s/%s: %v",
					env.Subscription, env.Environment, err)
			}
		}
	}
}

// performCurrentDayMonitoring scans current day (and previous day during EOD overlap)
func (s *BlobMonitorService) performCurrentDayMonitoring(ctx context.Context, env EnvironmentConfig) error {
	location, err := time.LoadLocation(s.config.Global.Timezone)
	if err != nil {
		return fmt.Errorf("invalid timezone: %w", err)
	}

	now := time.Now().In(location)
	today := now.Format("20060102")

	// Always scan today
	if err := s.scanEnvironmentForDate(ctx, env, today); err != nil {
		return fmt.Errorf("failed to scan today: %w", err)
	}

	// Check if we should also scan yesterday (EOD overlap period)
	inOverlap, err := s.config.IsInEODOverlapPeriod()
	if err != nil {
		log.Printf("⚠️ Failed to check EOD overlap: %v", err)
	} else if inOverlap {
		yesterday := now.AddDate(0, 0, -1).Format("20060102")
		log.Printf("🌅 In EOD overlap period, also scanning yesterday: %s", yesterday)

		if err := s.scanEnvironmentForDate(ctx, env, yesterday); err != nil {
			log.Printf("⚠️ Failed to scan yesterday during EOD overlap: %v", err)
		}
	}

	return nil
}

// scanEnvironmentForDate scans all selectors for a specific environment and date
func (s *BlobMonitorService) scanEnvironmentForDate(ctx context.Context, env EnvironmentConfig, date string) error {
	clientKey := fmt.Sprintf("%s-%s", env.Subscription, env.Environment)
	client, exists := s.storageClients[clientKey]
	if !exists {
		return fmt.Errorf("no Azure client for %s/%s", env.Subscription, env.Environment)
	}

	// Scan each configured selector
	for _, selectorName := range env.Selectors {
		selector, err := GetSelector(selectorName)
		if err != nil {
			return fmt.Errorf("failed to get selector %s: %w", selectorName, err)
		}

		if err := s.scanSelectorForDate(ctx, client, env, selector, date); err != nil {
			log.Printf("❌ Failed to scan selector %s for %s/%s on %s: %v",
				selectorName, env.Subscription, env.Environment, date, err)

			if !s.config.ErrorHandling.ContinueOnEnvironmentError {
				return err
			}
		}
	}

	return nil
}

// scanSelectorForDate scans blobs for a specific selector, environment, and date
func (s *BlobMonitorService) scanSelectorForDate(ctx context.Context, client *azblob.Client,
	env EnvironmentConfig, selector *BlobSelector, date string) error {

	// Capture listing start time BEFORE making the request (critical for consistency)
	listingStartTime := time.Now()

	log.Printf("🔍 Scanning %s/%s for %s on %s",
		env.Subscription, env.Environment, selector.Name, date)

	// List blobs with date prefix
	prefix := selector.GetDatePrefix(date)
	containerClient := client.ServiceClient().NewContainerClient(s.config.Storage.ContainerName)

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

	listingEndTime := time.Now()

	log.Printf("📦 Found %d %s blobs for %s/%s on %s (%.2f MB)",
		len(foundBlobs), selector.Name, env.Subscription, env.Environment, date,
		float64(totalBytes)/(1024*1024))

	// Publish BlobObserved events for each found blob
	for _, blob := range foundBlobs {
		event := BlobObservedEvent{
			Subscription:     env.Subscription,
			Environment:      env.Environment,
			BlobName:         *blob.Name,
			ObservationDate:  time.Now(),
			SizeInBytes:      *blob.Properties.ContentLength,
			LastModifiedDate: *blob.Properties.LastModified,
			ServiceSelector:  selector.Name,
		}

		if err := s.publishBlobObservedEvent(event); err != nil {
			log.Printf("❌ Failed to publish BlobObserved event for %s: %v", *blob.Name, err)
		}
	}

	// Publish BlobsListed completion event
	completionEvent := BlobsListedEvent{
		Subscription:     env.Subscription,
		Environment:      env.Environment,
		ServiceSelector:  selector.Name,
		Date:             date,
		ListingStartTime: listingStartTime,
		ListingEndTime:   listingEndTime,
		BlobCount:        len(foundBlobs),
		TotalBytes:       totalBytes,
	}

	if err := s.publishBlobsListedEvent(completionEvent); err != nil {
		return fmt.Errorf("failed to publish BlobsListed event: %w", err)
	}

	return nil
}

// creates a standardized key for blob events
// Format: {subscription}-{environment}-{cleanBlobName}-{suffix}
// This removes the kubernetes/ prefix and adds environment context
func generateBlobKey(subscription, environment, blobName, suffix string) string {
	// Remove kubernetes/ prefix if present
	cleanBlobName := blobName
	if strings.HasPrefix(blobName, "kubernetes/") {
		cleanBlobName = strings.TrimPrefix(blobName, "kubernetes/")
	}

	return fmt.Sprintf("%s-%s-%s-%s", subscription, environment, cleanBlobName, suffix)
}

// publishBlobObservedEvent publishes a BlobObserved event to Kafka
func (s *BlobMonitorService) publishBlobObservedEvent(event BlobObservedEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal BlobObserved event: %w", err)
	}

	key := generateBlobKey(event.Subscription, event.Environment, event.BlobName, "observed")

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &s.config.Kafka.Topic,
		},
		Key:   []byte(key),
		Value: eventJSON,
	}

	return s.kafkaProducer.Produce(message, nil)
}

// publishBlobsListedEvent publishes a BlobsListed completion event to Kafka
func (s *BlobMonitorService) publishBlobsListedEvent(event BlobsListedEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal BlobsListed event: %w", err)
	}

	key := fmt.Sprintf("%s-%s-%s-%s-listed",
		event.Subscription, event.Environment, event.ServiceSelector, event.Date)

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &s.config.Kafka.Topic,
		},
		Key:   []byte(key),
		Value: eventJSON,
	}

	return s.kafkaProducer.Produce(message, nil)
}

// handleKafkaDeliveryReports processes Kafka delivery confirmations
func (s *BlobMonitorService) handleKafkaDeliveryReports() {
	for e := range s.kafkaProducer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("❌ Kafka delivery failed for key %s: %v",
					string(ev.Key), ev.TopicPartition.Error)
			} else if s.config.Monitoring.LogLevel == "debug" {
				log.Printf("✅ Kafka delivery successful for key %s to %s[%d]@%v",
					string(ev.Key), *ev.TopicPartition.Topic,
					ev.TopicPartition.Partition, ev.TopicPartition.Offset)
			}
		case kafka.Error:
			log.Printf("❌ Kafka producer error: %v", ev)
		}
	}
}

// main function
func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <config-file>", os.Args[0])
	}

	configPath := os.Args[1]

	service, err := NewBlobMonitorService(configPath)
	if err != nil {
		log.Fatalf("❌ Failed to create blob monitor service: %v", err)
	}

	ctx := context.Background()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("🛑 Received signal %v, initiating graceful shutdown...", sig)
		service.Stop()
	}()

	if err := service.Start(ctx); err != nil {
		log.Fatalf("❌ Service failed: %v", err)
	}
}
