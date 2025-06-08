package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the complete configuration for the blob monitor service
type Config struct {
	Kafka         KafkaConfig         `yaml:"kafka"`
	Global        GlobalConfig        `yaml:"global"`
	DateRange     DateRangeConfig     `yaml:"date_range"`
	Environments  []EnvironmentConfig `yaml:"environments"`
	Storage       StorageConfig       `yaml:"storage"`
	Monitoring    MonitoringConfig    `yaml:"monitoring"`
	ErrorHandling ErrorHandlingConfig `yaml:"error_handling"`
}

// KafkaConfig defines Kafka connection and producer settings
type KafkaConfig struct {
	Brokers        string                 `yaml:"brokers"`
	Topic          string                 `yaml:"topic"`
	ProducerConfig map[string]interface{} `yaml:"producer_config"`
}

// GlobalConfig contains global service settings
type GlobalConfig struct {
	PollingInterval   int    `yaml:"polling_interval"`    // seconds
	EODOverlapMinutes int    `yaml:"eod_overlap_minutes"` // minutes
	Timezone          string `yaml:"timezone"`
}

// DateRangeConfig defines how far back to look for blobs
type DateRangeConfig struct {
	SpecificDate      *string `yaml:"specific_date,omitempty"` // "2025-06-01"
	DaysBack          *int    `yaml:"days_back,omitempty"`     // number of days
	MonitorCurrentDay bool    `yaml:"monitor_current_day"`
}

// EnvironmentConfig defines settings for a specific environment
type EnvironmentConfig struct {
	Subscription    string   `yaml:"subscription"`
	Environment     string   `yaml:"environment"`
	Enabled         bool     `yaml:"enabled"`
	PollingInterval *int     `yaml:"polling_interval,omitempty"` // override global, seconds
	Selectors       []string `yaml:"selectors"`
}

// StorageConfig defines Azure storage settings
type StorageConfig struct {
	ContainerName string `yaml:"container_name"`
}

// MonitoringConfig defines observability settings
type MonitoringConfig struct {
	EnableMetrics       bool   `yaml:"enable_metrics"`
	LogLevel            string `yaml:"log_level"`
	StatsReportInterval int    `yaml:"stats_report_interval"`
}

// ErrorHandlingConfig defines error handling behavior
type ErrorHandlingConfig struct {
	MaxAzureRetries            int    `yaml:"max_azure_retries"`
	RetryStrategy              string `yaml:"retry_strategy"`
	MaxRetryDelay              int    `yaml:"max_retry_delay"`
	ContinueOnEnvironmentError bool   `yaml:"continue_on_environment_error"`
}

// LoadConfig loads and validates the configuration from file
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

	return &config, nil
}

// validateConfig performs comprehensive validation of the configuration
func validateConfig(config *Config) error {
	// Validate Kafka configuration
	if config.Kafka.Brokers == "" {
		return fmt.Errorf("kafka.brokers is required")
	}
	if config.Kafka.Topic == "" {
		return fmt.Errorf("kafka.topic is required")
	}

	// Validate global configuration
	if config.Global.PollingInterval <= 0 {
		return fmt.Errorf("global.polling_interval must be positive")
	}
	if config.Global.EODOverlapMinutes < 0 {
		return fmt.Errorf("global.eod_overlap_minutes cannot be negative")
	}

	// Validate timezone
	if config.Global.Timezone == "" {
		config.Global.Timezone = "UTC" // default
	}
	if _, err := time.LoadLocation(config.Global.Timezone); err != nil {
		return fmt.Errorf("invalid timezone '%s': %w", config.Global.Timezone, err)
	}

	// Validate date range configuration
	if err := validateDateRange(&config.DateRange); err != nil {
		return fmt.Errorf("date_range validation failed: %w", err)
	}

	// Validate environments
	if len(config.Environments) == 0 {
		return fmt.Errorf("at least one environment must be configured")
	}

	for i, env := range config.Environments {
		if err := validateEnvironment(&env, i); err != nil {
			return fmt.Errorf("environment[%d] validation failed: %w", i, err)
		}
	}

	// Validate storage configuration
	if config.Storage.ContainerName == "" {
		return fmt.Errorf("storage.container_name is required")
	}

	// Validate monitoring configuration
	if config.Monitoring.LogLevel == "" {
		config.Monitoring.LogLevel = "info" // default
	}
	validLogLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLogLevels[config.Monitoring.LogLevel] {
		return fmt.Errorf("invalid log level '%s'. Valid levels: debug, info, warn, error", config.Monitoring.LogLevel)
	}

	// Validate error handling configuration
	if config.ErrorHandling.MaxAzureRetries < 0 {
		return fmt.Errorf("error_handling.max_azure_retries cannot be negative")
	}
	if config.ErrorHandling.MaxRetryDelay <= 0 {
		config.ErrorHandling.MaxRetryDelay = 60 // default
	}
	validStrategies := map[string]bool{"exponential": true, "linear": true, "fixed": true}
	if config.ErrorHandling.RetryStrategy != "" && !validStrategies[config.ErrorHandling.RetryStrategy] {
		return fmt.Errorf("invalid retry strategy '%s'. Valid strategies: exponential, linear, fixed", config.ErrorHandling.RetryStrategy)
	}

	return nil
}

// validateDateRange validates the date range configuration
func validateDateRange(dateRange *DateRangeConfig) error {
	if dateRange.SpecificDate != nil && dateRange.DaysBack != nil {
		return fmt.Errorf("cannot specify both specific_date and days_back")
	}

	if dateRange.SpecificDate == nil && dateRange.DaysBack == nil {
		return fmt.Errorf("must specify either specific_date or days_back")
	}

	if dateRange.SpecificDate != nil {
		// Validate date format
		if _, err := time.Parse("2006-01-02", *dateRange.SpecificDate); err != nil {
			return fmt.Errorf("invalid specific_date format '%s'. Use YYYY-MM-DD", *dateRange.SpecificDate)
		}
	}

	if dateRange.DaysBack != nil && *dateRange.DaysBack < 0 {
		return fmt.Errorf("days_back cannot be negative")
	}

	return nil
}

// validateEnvironment validates a single environment configuration
func validateEnvironment(env *EnvironmentConfig, index int) error {
	if env.Subscription == "" {
		return fmt.Errorf("subscription is required")
	}
	if env.Environment == "" {
		return fmt.Errorf("environment is required")
	}
	if !env.Enabled {
		return nil // Skip further validation for disabled environments
	}

	if len(env.Selectors) == 0 {
		return fmt.Errorf("at least one selector must be specified")
	}

	// Validate that all referenced selectors exist
	for j, selectorName := range env.Selectors {
		if err := ValidateSelector(selectorName); err != nil {
			return fmt.Errorf("selector[%d] '%s': %w", j, selectorName, err)
		}
	}

	if env.PollingInterval != nil && *env.PollingInterval <= 0 {
		return fmt.Errorf("polling_interval must be positive")
	}

	return nil
}

// GetPollingInterval returns the effective polling interval for an environment
func (env *EnvironmentConfig) GetPollingInterval(globalInterval int) int {
	if env.PollingInterval != nil {
		return *env.PollingInterval
	}
	return globalInterval
}

// GetStartDate returns the calculated start date based on the date range configuration
func (config *Config) GetStartDate() (time.Time, error) {
	location, err := time.LoadLocation(config.Global.Timezone)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid timezone: %w", err)
	}

	now := time.Now().In(location)

	if config.DateRange.SpecificDate != nil {
		startDate, err := time.ParseInLocation("2006-01-02", *config.DateRange.SpecificDate, location)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid specific_date: %w", err)
		}
		return startDate, nil
	}

	if config.DateRange.DaysBack != nil {
		startDate := now.AddDate(0, 0, -*config.DateRange.DaysBack)
		return startDate, nil
	}

	return time.Time{}, fmt.Errorf("no valid date range configuration")
}

// IsInEODOverlapPeriod checks if current time is in the end-of-day overlap period
func (config *Config) IsInEODOverlapPeriod() (bool, error) {
	location, err := time.LoadLocation(config.Global.Timezone)
	if err != nil {
		return false, fmt.Errorf("invalid timezone: %w", err)
	}

	now := time.Now().In(location)

	// Check if we're in the first N minutes of the day
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, location)
	overlapEnd := startOfDay.Add(time.Duration(config.Global.EODOverlapMinutes) * time.Minute)

	return now.Before(overlapEnd), nil
}
