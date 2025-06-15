package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the complete configuration for the extraction service
type Config struct {
	Kafka      KafkaConfig      `yaml:"kafka"`
	Processing ProcessingConfig `yaml:"processing"`
	Logging    LoggingConfig    `yaml:"logging"`
}

// KafkaConfig defines Kafka connection settings
type KafkaConfig struct {
	Brokers        string                 `yaml:"brokers"`
	InputTopic     string                 `yaml:"input_topic"`
	OutputTopic    string                 `yaml:"output_topic"`
	ErrorTopic     string                 `yaml:"error_topic"`
	ConsumerGroup  string                 `yaml:"consumer_group"`
	ConsumerConfig map[string]interface{} `yaml:"consumer_config"`
	ProducerConfig map[string]interface{} `yaml:"producer_config"`
	BatchSize      int                    `yaml:"batch_size"`
	FlushTimeoutMs int                    `yaml:"flush_timeout_ms"`
}

// ProcessingConfig defines log processing settings
type ProcessingConfig struct {
	MaxConcurrency   int  `yaml:"max_concurrency"`
	EnableValidation bool `yaml:"enable_validation"`
	SkipInvalidLogs  bool `yaml:"skip_invalid_logs"`
	LogParseErrors   bool `yaml:"log_parse_errors"`
}

// LoggingConfig defines logging settings
type LoggingConfig struct {
	Level           string `yaml:"level"`
	EnableMetrics   bool   `yaml:"enable_metrics"`
	MetricsInterval int    `yaml:"metrics_interval"`
}

// LoadConfig parses YAML configuration file and validates all settings
func LoadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config YAML: %w", err)
	}

	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	// Set defaults
	setDefaults(&config)

	return &config, nil
}

// validateConfig performs comprehensive validation to catch configuration errors
func validateConfig(config *Config) error {
	// Validate Kafka configuration
	if config.Kafka.Brokers == "" {
		return fmt.Errorf("kafka.brokers is required")
	}
	if config.Kafka.InputTopic == "" {
		return fmt.Errorf("kafka.input_topic is required")
	}
	if config.Kafka.OutputTopic == "" {
		return fmt.Errorf("kafka.output_topic is required")
	}
	if config.Kafka.ErrorTopic == "" {
		return fmt.Errorf("kafka.error_topic is required")
	}
	if config.Kafka.ConsumerGroup == "" {
		return fmt.Errorf("kafka.consumer_group is required")
	}

	// Validate processing configuration
	if config.Processing.MaxConcurrency < 1 {
		return fmt.Errorf("processing.max_concurrency must be at least 1")
	}
	// Note: For optimal performance, max_concurrency should match the number of input topic partitions
	// Raw.ApplicationLogs has 12 partitions, so 12 workers is recommended

	// Validate logging configuration
	validLogLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if config.Logging.Level != "" && !validLogLevels[config.Logging.Level] {
		return fmt.Errorf("invalid log level '%s'. Valid levels: debug, info, warn, error", config.Logging.Level)
	}

	return nil
}

// setDefaults applies default values for optional configuration fields
func setDefaults(config *Config) {
	// Kafka defaults
	if config.Kafka.BatchSize == 0 {
		config.Kafka.BatchSize = 100
	}
	if config.Kafka.FlushTimeoutMs == 0 {
		config.Kafka.FlushTimeoutMs = 5000
	}

	// Processing defaults - match Kafka partition count for optimal performance
	// Raw.ApplicationLogs has 12 partitions, so default to 12 workers
	if config.Processing.MaxConcurrency == 0 {
		config.Processing.MaxConcurrency = 12
	}

	// Logging defaults
	if config.Logging.Level == "" {
		config.Logging.Level = "info"
	}
	if config.Logging.MetricsInterval == 0 {
		config.Logging.MetricsInterval = 60
	}
}
