package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the ingestion worker configuration
type Config struct {
	// Service mode: "worker" or "cli"
	Mode string `yaml:"mode" env:"INGEST_MODE" default:"worker"`

	// CLI-specific configuration (for backwards compatibility)
	CLI CLIConfig `yaml:"cli"`

	// Worker-specific configuration
	Worker WorkerConfig `yaml:"worker"`

	// Kafka configuration
	Kafka KafkaConfig `yaml:"kafka"`

	// Storage configuration placeholder (for dependency injection)
	Storage StorageConfig `yaml:"storage"`

	// Logging configuration
	LogLevel string `yaml:"log_level" env:"LOG_LEVEL" default:"info"`
}

// StorageConfig contains storage configuration (empty placeholder for dependency injection)
type StorageConfig struct {
	// This is intentionally empty - storage configuration is handled by the main config module
	// This struct exists to maintain interface compatibility for dependency injection
}

// CLIConfig contains configuration for CLI mode (backwards compatibility)
type CLIConfig struct {
	SubscriptionID  string `yaml:"subscription_id" env:"SUBSCRIPTION_ID"`
	Environment     string `yaml:"environment" env:"ENVIRONMENT"`
	ContainerName   string `yaml:"container_name" env:"AZURE_STORAGE_CONTAINER_NAME"`
	BlobName        string `yaml:"blob_name" env:"AZURE_STORAGE_BLOB_NAME"`
	StartOffset     int64  `yaml:"start_offset" env:"START_OFFSET" default:"0"`
	ServiceSelector string `yaml:"service_selector" env:"SERVICE_SELECTOR"`
}

// WorkerConfig contains configuration for worker mode
type WorkerConfig struct {
	// Blob state topic to consume
	BlobStateTopic string `yaml:"blob_state_topic" env:"BLOB_STATE_TOPIC" default:"Ingestion.BlobState"`

	// Consumer group for blob state topic
	ConsumerGroup string `yaml:"consumer_group" env:"CONSUMER_GROUP" default:"ingestion-worker"`

	// Filters
	Filters FilterConfig `yaml:"filters"`

	// Sharding configuration
	Sharding ShardingConfig `yaml:"sharding"`

	// Processing configuration
	ProcessingConfig ProcessingConfig `yaml:"processing"`
}

// FilterConfig contains filtering options
type FilterConfig struct {
	// Date range filters (YYYY-MM-DD format)
	MinDate *string `yaml:"min_date" env:"MIN_DATE"`
	MaxDate *string `yaml:"max_date" env:"MAX_DATE"`

	// Subscription filter (empty means all)
	Subscriptions []string `yaml:"subscriptions" env:"SUBSCRIPTIONS"`

	// Environment filter (empty means all)
	Environments []string `yaml:"environments" env:"ENVIRONMENTS"`

	// Selector filter - regex patterns (empty means all)
	Selectors []string `yaml:"selectors" env:"SELECTORS"`
}

// ShardingConfig contains sharding options
type ShardingConfig struct {
	// Enable sharding
	Enabled bool `yaml:"enabled" env:"SHARDING_ENABLED" default:"false"`

	// Total number of shards
	ShardsCount int `yaml:"shards_count" env:"SHARDS_COUNT" default:"1"`

	// This worker's shard number (0-based)
	ShardNumber int `yaml:"shard_number" env:"SHARD_NUMBER" default:"0"`
}

// ProcessingConfig contains blob processing configuration
type ProcessingConfig struct {
	LoopInterval   time.Duration  `yaml:"loop_interval"`    // Time between processing iterations
	LineBufferSize int            `yaml:"line_buffer_size"` // Buffer size for line scanning
	ParallelConfig ParallelConfig `yaml:"parallel"`         // Parallel processing configuration
}

// ParallelConfig contains parallel processing configuration
type ParallelConfig struct {
	Enabled        bool   `yaml:"enabled"`         // Enable parallel blob processing
	MaxConcurrency int    `yaml:"max_concurrency"` // Maximum concurrent blob downloads
	ChunkSizeHint  int64  `yaml:"chunk_size_hint"` // Suggested chunk size for large blobs (bytes)
	UseChunking    bool   `yaml:"use_chunking"`    // Enable chunking for large blobs
	ChunkThreshold int64  `yaml:"chunk_threshold"` // Minimum blob size to consider for chunking
	Environment    string `yaml:"environment"`     // Environment-specific settings (e.g., "D1", "P1")
}

// KafkaConfig contains Kafka connection settings
type KafkaConfig struct {
	Brokers string `yaml:"brokers" env:"KAFKA_BROKERS" default:"localhost:9092"`

	// Topic for ingested logs (deprecated, use ProxyTopic/ApplicationTopic)
	IngestTopic string `yaml:"ingest_topic" env:"KAFKA_INGEST_TOPIC" default:"Ingestion.RawLogs"`

	// Topic for proxy log lines
	ProxyTopic string `yaml:"proxy_topic" env:"KAFKA_PROXY_TOPIC" default:"Raw.ProxyLogs"`

	// Topic for application log lines
	ApplicationTopic string `yaml:"app_topic" env:"KAFKA_APP_TOPIC" default:"Raw.ApplicationLogs"`

	// Number of partitions for raw log topics
	Partitions int `yaml:"partitions" env:"KAFKA_RAW_PARTITIONS" default:"12"`

	// Topic for blob completion events
	BlobsTopic string `yaml:"blobs_topic" env:"KAFKA_BLOBS_TOPIC" default:"Ingestion.Blobs"`

	// Producer configuration
	Producer ProducerConfig `yaml:"producer"`

	// Consumer configuration
	Consumer ConsumerConfig `yaml:"consumer"`
}

// ProducerConfig contains Kafka producer settings
type ProducerConfig struct {
	Acks                string `yaml:"acks" env:"KAFKA_PRODUCER_ACKS" default:"all"`
	FlushTimeoutMs      int    `yaml:"flush_timeout_ms" env:"KAFKA_PRODUCER_FLUSH_TIMEOUT_MS" default:"120000"`
	DeliveryChannelSize int    `yaml:"delivery_channel_size" env:"KAFKA_PRODUCER_DELIVERY_CHANNEL_SIZE" default:"10000"`
}

// ConsumerConfig contains Kafka consumer settings
type ConsumerConfig struct {
	AutoOffsetReset  string `yaml:"auto_offset_reset" env:"KAFKA_CONSUMER_AUTO_OFFSET_RESET" default:"earliest"`
	EnableAutoCommit bool   `yaml:"enable_auto_commit" env:"KAFKA_CONSUMER_ENABLE_AUTO_COMMIT" default:"false"`
}

// Validate validates the configuration
func (c *Config) Validate() error {
	switch c.Mode {
	case "cli":
		return c.validateCLI()
	case "worker":
		return c.validateWorker()
	default:
		return fmt.Errorf("invalid mode %q, must be 'cli' or 'worker'", c.Mode)
	}
}

// validateCLI validates CLI mode configuration
func (c *Config) validateCLI() error {
	if c.CLI.SubscriptionID == "" {
		return fmt.Errorf("CLI mode requires subscription_id")
	}
	if c.CLI.Environment == "" {
		return fmt.Errorf("CLI mode requires environment")
	}
	if c.CLI.ContainerName == "" {
		return fmt.Errorf("CLI mode requires container_name")
	}
	if c.CLI.BlobName == "" {
		return fmt.Errorf("CLI mode requires blob_name")
	}
	if c.CLI.StartOffset < 0 {
		return fmt.Errorf("start_offset cannot be negative")
	}
	return c.validateCommon()
}

// validateWorker validates worker mode configuration
func (c *Config) validateWorker() error {
	if c.Worker.BlobStateTopic == "" {
		return fmt.Errorf("worker mode requires blob_state_topic")
	}
	if c.Worker.ConsumerGroup == "" {
		return fmt.Errorf("worker mode requires consumer_group")
	}

	// Validate sharding configuration
	if c.Worker.Sharding.Enabled {
		if c.Worker.Sharding.ShardsCount <= 0 {
			return fmt.Errorf("shards_count must be positive when sharding is enabled")
		}
		if c.Worker.Sharding.ShardNumber < 0 || c.Worker.Sharding.ShardNumber >= c.Worker.Sharding.ShardsCount {
			return fmt.Errorf("shard_number must be between 0 and shards_count-1")
		}
	}

	// Validate processing config
	if c.Worker.ProcessingConfig.LineBufferSize <= 0 {
		return fmt.Errorf("line_buffer_size must be positive")
	}

	// Validate date filters if provided
	if c.Worker.Filters.MinDate != nil {
		if _, err := time.Parse("2006-01-02", *c.Worker.Filters.MinDate); err != nil {
			return fmt.Errorf("min_date must be in YYYY-MM-DD format: %w", err)
		}
	}
	if c.Worker.Filters.MaxDate != nil {
		if _, err := time.Parse("2006-01-02", *c.Worker.Filters.MaxDate); err != nil {
			return fmt.Errorf("max_date must be in YYYY-MM-DD format: %w", err)
		}
	}

	return c.validateCommon()
}

// validateCommon validates common configuration
func (c *Config) validateCommon() error {
	if c.Kafka.Brokers == "" {
		return fmt.Errorf("kafka brokers are required")
	}
	if c.Kafka.ProxyTopic == "" {
		return fmt.Errorf("proxy_topic is required")
	}
	if c.Kafka.ApplicationTopic == "" {
		return fmt.Errorf("app_topic is required")
	}
	if c.Kafka.Partitions <= 0 {
		return fmt.Errorf("partitions must be positive")
	}
	return nil
}

// LoadConfigFromFile loads configuration from a YAML file
func LoadConfigFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}

	// Apply defaults and environment overrides
	applyDefaults(&cfg)

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// LoadConfigFromEnv loads configuration from environment variables with defaults
func LoadConfigFromEnv() (*Config, error) {
	cfg := Config{
		Mode:     getEnv("INGEST_MODE", "worker"),
		LogLevel: getEnv("LOG_LEVEL", "info"),
		CLI: CLIConfig{
			SubscriptionID:  os.Getenv("SUBSCRIPTION_ID"),
			Environment:     os.Getenv("ENVIRONMENT"),
			ContainerName:   os.Getenv("AZURE_STORAGE_CONTAINER_NAME"),
			BlobName:        os.Getenv("AZURE_STORAGE_BLOB_NAME"),
			StartOffset:     parseInt64Env("START_OFFSET", 0),
			ServiceSelector: os.Getenv("SERVICE_SELECTOR"),
		},
		Worker: WorkerConfig{
			BlobStateTopic: getEnv("BLOB_STATE_TOPIC", "Ingestion.BlobState"),
			ConsumerGroup:  getEnv("CONSUMER_GROUP", "ingestion-worker"),
			Filters: FilterConfig{
				MinDate:       getStringPtr(os.Getenv("MIN_DATE")),
				MaxDate:       getStringPtr(os.Getenv("MAX_DATE")),
				Subscriptions: parseStringSliceEnv("SUBSCRIPTIONS"),
				Environments:  parseStringSliceEnv("ENVIRONMENTS"),
				Selectors:     parseStringSliceEnv("SELECTORS"),
			},
			Sharding: ShardingConfig{
				Enabled:     parseBoolEnv("SHARDING_ENABLED", false),
				ShardsCount: parseIntEnv("SHARDS_COUNT", 1),
				ShardNumber: parseIntEnv("SHARD_NUMBER", 0),
			},
			ProcessingConfig: ProcessingConfig{
				LoopInterval:   parseDurationEnv("LOOP_INTERVAL", 30*time.Second),
				LineBufferSize: parseIntEnv("LINE_BUFFER_SIZE", 1048576),
			},
		},
		Kafka: KafkaConfig{
			Brokers:          getEnv("KAFKA_BROKERS", "localhost:9092"),
			IngestTopic:      getEnv("KAFKA_INGEST_TOPIC", "Ingestion.RawLogs"),
			ProxyTopic:       getEnv("KAFKA_PROXY_TOPIC", "Raw.ProxyLogs"),
			ApplicationTopic: getEnv("KAFKA_APP_TOPIC", "Raw.ApplicationLogs"),
			Partitions:       parseIntEnv("KAFKA_RAW_PARTITIONS", 12),
			BlobsTopic:       getEnv("KAFKA_BLOBS_TOPIC", "Ingestion.Blobs"),
			Producer: ProducerConfig{
				Acks:                getEnv("KAFKA_PRODUCER_ACKS", "all"),
				FlushTimeoutMs:      parseIntEnv("KAFKA_PRODUCER_FLUSH_TIMEOUT_MS", 120000),
				DeliveryChannelSize: parseIntEnv("KAFKA_PRODUCER_DELIVERY_CHANNEL_SIZE", 10000),
			},
			Consumer: ConsumerConfig{
				AutoOffsetReset:  getEnv("KAFKA_CONSUMER_AUTO_OFFSET_RESET", "earliest"),
				EnableAutoCommit: parseBoolEnv("KAFKA_CONSUMER_ENABLE_AUTO_COMMIT", false),
			},
		},
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// Helper functions for parsing environment variables
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getStringPtr(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func parseStringSliceEnv(key string) []string {
	value := os.Getenv(key)
	if value == "" {
		return nil
	}
	// Split by comma and trim spaces
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func parseIntEnv(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	if parsed, err := strconv.Atoi(value); err == nil {
		return parsed
	}
	return defaultValue
}

func parseInt64Env(key string, defaultValue int64) int64 {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
		return parsed
	}
	return defaultValue
}

func parseBoolEnv(key string, defaultValue bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	if parsed, err := strconv.ParseBool(value); err == nil {
		return parsed
	}
	return defaultValue
}

func parseDurationEnv(key string, defaultValue time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	if parsed, err := time.ParseDuration(value); err == nil {
		return parsed
	}
	return defaultValue
}

func applyDefaults(cfg *Config) {
	if cfg.Mode == "" {
		cfg.Mode = "worker"
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.Kafka.Brokers == "" {
		cfg.Kafka.Brokers = "localhost:9092"
	}
	if cfg.Kafka.IngestTopic == "" {
		cfg.Kafka.IngestTopic = "Ingestion.RawLogs"
	}
	if cfg.Kafka.ProxyTopic == "" {
		cfg.Kafka.ProxyTopic = "Raw.ProxyLogs"
	}
	if cfg.Kafka.ApplicationTopic == "" {
		cfg.Kafka.ApplicationTopic = "Raw.ApplicationLogs"
	}
	if cfg.Kafka.Partitions == 0 {
		cfg.Kafka.Partitions = 12
	}
	if cfg.Kafka.BlobsTopic == "" {
		cfg.Kafka.BlobsTopic = "Ingestion.Blobs"
	}
	if cfg.Kafka.Producer.Acks == "" {
		cfg.Kafka.Producer.Acks = "all"
	}
	if cfg.Kafka.Producer.FlushTimeoutMs == 0 {
		cfg.Kafka.Producer.FlushTimeoutMs = 30000
	}
	if cfg.Kafka.Producer.DeliveryChannelSize == 0 {
		cfg.Kafka.Producer.DeliveryChannelSize = 10000
	}
	if cfg.Kafka.Consumer.AutoOffsetReset == "" {
		cfg.Kafka.Consumer.AutoOffsetReset = "earliest"
	}
	if cfg.Worker.BlobStateTopic == "" {
		cfg.Worker.BlobStateTopic = "Ingestion.BlobState"
	}
	if cfg.Worker.ConsumerGroup == "" {
		cfg.Worker.ConsumerGroup = "ingestion-worker"
	}
	if cfg.Worker.Sharding.ShardsCount == 0 {
		cfg.Worker.Sharding.ShardsCount = 1
	}
	if cfg.Worker.ProcessingConfig.LoopInterval == 0 {
		cfg.Worker.ProcessingConfig.LoopInterval = 30 * time.Second
	}
	if cfg.Worker.ProcessingConfig.LineBufferSize == 0 {
		cfg.Worker.ProcessingConfig.LineBufferSize = 1048576 // 1MB
	}
}
