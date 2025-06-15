package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate_CLI_Mode(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		expectError bool
	}{
		{
			name: "valid CLI config",
			config: &Config{
				Mode: "cli",
				CLI: CLIConfig{
					SubscriptionID: "test-sub",
					Environment:    "test-env",
					ContainerName:  "test-container",
					BlobName:       "test-blob.gz",
					StartOffset:    0,
				},
				Kafka: KafkaConfig{
					Brokers: "localhost:9092",
				},
			},
			expectError: false,
		},
		{
			name: "missing subscription ID",
			config: &Config{
				Mode: "cli",
				CLI: CLIConfig{
					Environment:   "test-env",
					ContainerName: "test-container",
					BlobName:      "test-blob.gz",
				},
				Kafka: KafkaConfig{
					Brokers: "localhost:9092",
				},
			},
			expectError: true,
		},
		{
			name: "missing environment",
			config: &Config{
				Mode: "cli",
				CLI: CLIConfig{
					SubscriptionID: "test-sub",
					ContainerName:  "test-container",
					BlobName:       "test-blob.gz",
				},
				Kafka: KafkaConfig{
					Brokers: "localhost:9092",
				},
			},
			expectError: true,
		},
		{
			name: "negative start offset",
			config: &Config{
				Mode: "cli",
				CLI: CLIConfig{
					SubscriptionID: "test-sub",
					Environment:    "test-env",
					ContainerName:  "test-container",
					BlobName:       "test-blob.gz",
					StartOffset:    -1,
				},
				Kafka: KafkaConfig{
					Brokers: "localhost:9092",
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfig_Validate_Worker_Mode(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		expectError bool
	}{
		{
			name: "valid worker config",
			config: &Config{
				Mode: "worker",
				Worker: WorkerConfig{
					BlobStateTopic: "test-topic",
					ConsumerGroup:  "test-group",
					ProcessingConfig: ProcessingConfig{
						LineBufferSize: 1024,
					},
				},
				Kafka: KafkaConfig{
					Brokers: "localhost:9092",
				},
			},
			expectError: false,
		},
		{
			name: "missing blob state topic",
			config: &Config{
				Mode: "worker",
				Worker: WorkerConfig{
					ConsumerGroup: "test-group",
				},
				Kafka: KafkaConfig{
					Brokers: "localhost:9092",
				},
			},
			expectError: true,
		},
		{
			name: "invalid sharding config",
			config: &Config{
				Mode: "worker",
				Worker: WorkerConfig{
					BlobStateTopic: "test-topic",
					ConsumerGroup:  "test-group",
					Sharding: ShardingConfig{
						Enabled:     true,
						ShardsCount: -1,
					},
				},
				Kafka: KafkaConfig{
					Brokers: "localhost:9092",
				},
			},
			expectError: true,
		},
		{
			name: "invalid date filter",
			config: &Config{
				Mode: "worker",
				Worker: WorkerConfig{
					BlobStateTopic: "test-topic",
					ConsumerGroup:  "test-group",
					Filters: FilterConfig{
						MinDate: stringPtr("invalid-date"),
					},
				},
				Kafka: KafkaConfig{
					Brokers: "localhost:9092",
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	// Save original env vars
	originalVars := map[string]string{
		"INGEST_MODE":                  os.Getenv("INGEST_MODE"),
		"SUBSCRIPTION_ID":              os.Getenv("SUBSCRIPTION_ID"),
		"ENVIRONMENT":                  os.Getenv("ENVIRONMENT"),
		"AZURE_STORAGE_CONTAINER_NAME": os.Getenv("AZURE_STORAGE_CONTAINER_NAME"),
		"AZURE_STORAGE_BLOB_NAME":      os.Getenv("AZURE_STORAGE_BLOB_NAME"),
		"KAFKA_BROKERS":                os.Getenv("KAFKA_BROKERS"),
	}

	// Cleanup function
	cleanup := func() {
		for key, value := range originalVars {
			if value == "" {
				os.Unsetenv(key)
			} else {
				os.Setenv(key, value)
			}
		}
	}
	defer cleanup()

	t.Run("loads CLI config from environment", func(t *testing.T) {
		// Set test environment variables
		os.Setenv("INGEST_MODE", "cli")
		os.Setenv("SUBSCRIPTION_ID", "test-sub")
		os.Setenv("ENVIRONMENT", "test-env")
		os.Setenv("AZURE_STORAGE_CONTAINER_NAME", "test-container")
		os.Setenv("AZURE_STORAGE_BLOB_NAME", "test-blob.gz")
		os.Setenv("KAFKA_BROKERS", "localhost:9092")

		cfg, err := LoadConfigFromEnv()
		require.NoError(t, err)

		assert.Equal(t, "cli", cfg.Mode)
		assert.Equal(t, "test-sub", cfg.CLI.SubscriptionID)
		assert.Equal(t, "test-env", cfg.CLI.Environment)
		assert.Equal(t, "test-container", cfg.CLI.ContainerName)
		assert.Equal(t, "test-blob.gz", cfg.CLI.BlobName)
		assert.Equal(t, "localhost:9092", cfg.Kafka.Brokers)
	})

	t.Run("applies defaults correctly", func(t *testing.T) {
		// Clear all env vars
		for key := range originalVars {
			os.Unsetenv(key)
		}

		cfg, err := LoadConfigFromEnv()
		require.NoError(t, err)

		// Check defaults
		assert.Equal(t, "worker", cfg.Mode)
		assert.Equal(t, "localhost:9092", cfg.Kafka.Brokers)
		assert.Equal(t, "Raw.ProxyLogs", cfg.Kafka.ProxyTopic)
		assert.Equal(t, "Raw.ApplicationLogs", cfg.Kafka.ApplicationTopic)
		assert.Equal(t, 12, cfg.Kafka.Partitions)
		assert.Equal(t, time.Duration(30)*time.Second, cfg.Worker.ProcessingConfig.LoopInterval)
	})
}

func TestParseStringSliceEnv(t *testing.T) {
	tests := []struct {
		name     string
		envVar   string
		expected []string
	}{
		{
			name:     "empty string returns nil slice",
			envVar:   "",
			expected: nil,
		},
		{
			name:     "single item",
			envVar:   "item1",
			expected: []string{"item1"},
		},
		{
			name:     "multiple items",
			envVar:   "item1,item2,item3",
			expected: []string{"item1", "item2", "item3"},
		},
		{
			name:     "items with spaces",
			envVar:   "item1, item2 , item3",
			expected: []string{"item1", "item2", "item3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("TEST_SLICE", tt.envVar)
			defer os.Unsetenv("TEST_SLICE")

			result := parseStringSliceEnv("TEST_SLICE")
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseDurationEnv(t *testing.T) {
	tests := []struct {
		name         string
		envVar       string
		defaultValue time.Duration
		expected     time.Duration
	}{
		{
			name:         "valid duration",
			envVar:       "45s",
			defaultValue: 30 * time.Second,
			expected:     45 * time.Second,
		},
		{
			name:         "invalid duration uses default",
			envVar:       "invalid",
			defaultValue: 30 * time.Second,
			expected:     30 * time.Second,
		},
		{
			name:         "empty string uses default",
			envVar:       "",
			defaultValue: 30 * time.Second,
			expected:     30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envVar != "" {
				os.Setenv("TEST_DURATION", tt.envVar)
				defer os.Unsetenv("TEST_DURATION")
			}

			result := parseDurationEnv("TEST_DURATION", tt.defaultValue)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Helper function
func stringPtr(s string) *string {
	return &s
}
