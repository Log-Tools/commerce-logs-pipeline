package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Verifies complete configuration parsing from YAML with all sections populated
func TestLoadConfig_Success(t *testing.T) {
	// Create a temporary config file
	configContent := `
kafka:
  brokers: "localhost:9092"
  topic: "test-topic"
  producer_config:
    acks: "all"
    retries: 3

global:
  polling_interval: 300
  eod_overlap_minutes: 60
  timezone: "UTC"

date_range:
  days_back: 3
  monitor_current_day: true

environments:
  - subscription: "cp2"
    environment: "D1"
    enabled: true
    selectors:
      - "apache-proxy"
      - "api"

storage:
  container_name: "test-container"

monitoring:
  log_level: "info"
  enable_metrics: true

error_handling:
  max_azure_retries: 3
  continue_on_environment_error: true
`

	tmpFile, err := os.CreateTemp("", "test-config-*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(configContent)
	require.NoError(t, err)
	tmpFile.Close()

	// Test loading
	config, err := LoadConfig(tmpFile.Name())
	require.NoError(t, err)
	require.NotNil(t, config)

	// Verify loaded configuration
	assert.Equal(t, "localhost:9092", config.Kafka.Brokers)
	assert.Equal(t, "test-topic", config.Kafka.Topic)
	assert.Equal(t, 300, config.Global.PollingInterval)
	assert.Equal(t, "UTC", config.Global.Timezone)
	assert.NotNil(t, config.DateRange.DaysBack)
	assert.Equal(t, 3, *config.DateRange.DaysBack)
	assert.True(t, config.DateRange.MonitorCurrentDay)
	assert.Len(t, config.Environments, 1)
	assert.Equal(t, "cp2", config.Environments[0].Subscription)
	assert.True(t, config.Environments[0].Enabled)
}

// Verifies proper error handling when configuration file doesn't exist
func TestLoadConfig_FileNotFound(t *testing.T) {
	_, err := LoadConfig("nonexistent.yaml")
	assert.Error(t, err)
}

// Verifies proper error handling for malformed YAML content
func TestLoadConfig_InvalidYAML(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "invalid-*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString("invalid: yaml: content: [")
	require.NoError(t, err)
	tmpFile.Close()

	_, err = LoadConfig(tmpFile.Name())
	assert.Error(t, err)
}

// Ensures validation catches missing required broker configuration
func TestConfigValidation_MissingKafkaBrokers(t *testing.T) {
	config := &Config{
		Kafka: KafkaConfig{
			Topic: "test-topic",
		},
	}

	err := validateConfig(config)
	assert.Error(t, err)
}

// Ensures validation catches missing required topic configuration
func TestConfigValidation_MissingKafkaTopic(t *testing.T) {
	config := &Config{
		Kafka: KafkaConfig{
			Brokers: "localhost:9092",
		},
	}

	err := validateConfig(config)
	assert.Error(t, err)
}

// Validates rejection of negative polling intervals that would break timing logic
func TestConfigValidation_InvalidPollingInterval(t *testing.T) {
	config := &Config{
		Kafka: KafkaConfig{
			Brokers: "localhost:9092",
			Topic:   "test-topic",
		},
		Global: GlobalConfig{
			PollingInterval: -1,
		},
	}

	err := validateConfig(config)
	assert.Error(t, err)
}

// Validates rejection of timezone strings that would cause runtime errors
func TestConfigValidation_InvalidTimezone(t *testing.T) {
	config := &Config{
		Kafka: KafkaConfig{
			Brokers: "localhost:9092",
			Topic:   "test-topic",
		},
		Global: GlobalConfig{
			PollingInterval: 300,
			Timezone:        "Invalid/Timezone",
		},
	}

	err := validateConfig(config)
	assert.Error(t, err)
}

// Validates mutually exclusive date range options and format requirements
func TestDateRangeValidation(t *testing.T) {
	tests := []struct {
		name        string
		dateRange   DateRangeConfig
		expectError bool
	}{
		{
			name: "valid days_back",
			dateRange: DateRangeConfig{
				DaysBack: intPtr(3),
			},
			expectError: false,
		},
		{
			name: "valid specific_date",
			dateRange: DateRangeConfig{
				SpecificDate: stringPtr("2025-06-01"),
			},
			expectError: false,
		},
		{
			name: "both specified - error",
			dateRange: DateRangeConfig{
				DaysBack:     intPtr(3),
				SpecificDate: stringPtr("2025-06-01"),
			},
			expectError: true,
		},
		{
			name:        "neither specified - error",
			dateRange:   DateRangeConfig{},
			expectError: true,
		},
		{
			name: "invalid date format",
			dateRange: DateRangeConfig{
				SpecificDate: stringPtr("2025/06/01"),
			},
			expectError: true,
		},
		{
			name: "negative days_back",
			dateRange: DateRangeConfig{
				DaysBack: intPtr(-1),
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDateRange(&tt.dateRange)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Validates required fields and logical constraints per environment configuration
func TestEnvironmentValidation(t *testing.T) {
	tests := []struct {
		name        string
		env         EnvironmentConfig
		expectError bool
	}{
		{
			name: "valid environment",
			env: EnvironmentConfig{
				Subscription: "cp2",
				Environment:  "D1",
				Enabled:      true,
				Selectors:    []string{"apache-proxy"},
			},
			expectError: false,
		},
		{
			name: "missing subscription",
			env: EnvironmentConfig{
				Environment: "D1",
				Enabled:     true,
				Selectors:   []string{"apache-proxy"},
			},
			expectError: true,
		},
		{
			name: "missing environment",
			env: EnvironmentConfig{
				Subscription: "cp2",
				Enabled:      true,
				Selectors:    []string{"apache-proxy"},
			},
			expectError: true,
		},
		{
			name: "disabled environment - skip validation",
			env: EnvironmentConfig{
				Subscription: "cp2",
				Environment:  "D1",
				Enabled:      false,
				// Missing selectors should be ok for disabled env
			},
			expectError: false,
		},
		{
			name: "missing selectors for enabled env",
			env: EnvironmentConfig{
				Subscription: "cp2",
				Environment:  "D1",
				Enabled:      true,
				Selectors:    []string{},
			},
			expectError: true,
		},
		{
			name: "invalid polling interval",
			env: EnvironmentConfig{
				Subscription:    "cp2",
				Environment:     "D1",
				Enabled:         true,
				Selectors:       []string{"apache-proxy"},
				PollingInterval: intPtr(-1),
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateEnvironment(&tt.env, 0)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Verifies environment-specific polling overrides take precedence over global values
func TestGetPollingInterval(t *testing.T) {
	tests := []struct {
		name           string
		env            EnvironmentConfig
		globalInterval int
		expected       int
	}{
		{
			name: "use environment override",
			env: EnvironmentConfig{
				PollingInterval: intPtr(600),
			},
			globalInterval: 300,
			expected:       600,
		},
		{
			name:           "use global default",
			env:            EnvironmentConfig{},
			globalInterval: 300,
			expected:       300,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.env.GetPollingInterval(tt.globalInterval)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Validates timezone-aware date calculation for both specific dates and relative ranges
func TestGetStartDate(t *testing.T) {
	tests := []struct {
		name      string
		config    Config
		expectErr bool
	}{
		{
			name: "specific date",
			config: Config{
				DateRange: DateRangeConfig{
					SpecificDate: stringPtr("2025-06-01"),
				},
				Global: GlobalConfig{
					Timezone: "UTC",
				},
			},
			expectErr: false,
		},
		{
			name: "days back",
			config: Config{
				DateRange: DateRangeConfig{
					DaysBack: intPtr(3),
				},
				Global: GlobalConfig{
					Timezone: "UTC",
				},
			},
			expectErr: false,
		},
		{
			name: "invalid timezone",
			config: Config{
				DateRange: DateRangeConfig{
					DaysBack: intPtr(3),
				},
				Global: GlobalConfig{
					Timezone: "Invalid/Timezone",
				},
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startDate, err := tt.config.GetStartDate()
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.False(t, startDate.IsZero())
			}
		})
	}
}

// Validates end-of-day overlap detection for handling cross-midnight blob processing
func TestIsInEODOverlapPeriod(t *testing.T) {
	config := &Config{
		Global: GlobalConfig{
			EODOverlapMinutes: 60, // 1 hour
			Timezone:          "UTC",
		},
	}

	// Test with current time (this is time-dependent, so we just ensure no error)
	inOverlap, err := config.IsInEODOverlapPeriod()
	assert.NoError(t, err)
	assert.IsType(t, true, inOverlap) // Just verify it returns a boolean

	// Test with invalid timezone
	config.Global.Timezone = "Invalid/Timezone"
	_, err = config.IsInEODOverlapPeriod()
	assert.Error(t, err)
}

// Ensures only valid log levels are accepted to prevent runtime logging issues
func TestLogLevelValidation(t *testing.T) {
	validConfig := &Config{
		Kafka: KafkaConfig{
			Brokers: "localhost:9092",
			Topic:   "test-topic",
		},
		Global: GlobalConfig{
			PollingInterval: 300,
		},
		DateRange: DateRangeConfig{
			DaysBack: intPtr(1),
		},
		Environments: []EnvironmentConfig{
			{
				Subscription: "test",
				Environment:  "test",
				Enabled:      true,
				Selectors:    []string{"apache-proxy"},
			},
		},
		Storage: StorageConfig{
			ContainerName: "test",
		},
		Monitoring: MonitoringConfig{
			LogLevel: "invalid-level",
		},
	}

	err := validateConfig(validConfig)
	assert.Error(t, err)

	// Test valid log levels
	validLevels := []string{"debug", "info", "warn", "error"}
	for _, level := range validLevels {
		validConfig.Monitoring.LogLevel = level
		err := validateConfig(validConfig)
		assert.NoError(t, err, "Log level %s should be valid", level)
	}
}

// Validates blob closing configuration accepts valid settings and rejects invalid ones
func TestBlobClosingConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      BlobClosingConfig
		expectError bool
	}{
		{
			name: "valid enabled config",
			config: BlobClosingConfig{
				Enabled:        true,
				TimeoutMinutes: 5,
			},
			expectError: false,
		},
		{
			name: "valid disabled config",
			config: BlobClosingConfig{
				Enabled:        false,
				TimeoutMinutes: 0, // Should be ignored when disabled
			},
			expectError: false,
		},
		{
			name: "invalid timeout when enabled",
			config: BlobClosingConfig{
				Enabled:        true,
				TimeoutMinutes: 0,
			},
			expectError: true,
		},
		{
			name: "negative timeout when enabled",
			config: BlobClosingConfig{
				Enabled:        true,
				TimeoutMinutes: -5,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{
				Kafka: KafkaConfig{
					Brokers: "localhost:9092",
					Topic:   "test-topic",
				},
				Global: GlobalConfig{
					PollingInterval:   300,
					BlobClosingConfig: tt.config,
				},
				DateRange: DateRangeConfig{
					DaysBack: intPtr(1),
				},
				Environments: []EnvironmentConfig{
					{
						Subscription: "test",
						Environment:  "test",
						Enabled:      true,
						Selectors:    []string{"apache-proxy"},
					},
				},
				Storage: StorageConfig{
					ContainerName: "test",
				},
				Monitoring: MonitoringConfig{
					LogLevel: "info",
				},
			}

			err := validateConfig(config)
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "blob_closing")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Helper functions
func intPtr(i int) *int {
	return &i
}

func stringPtr(s string) *string {
	return &s
}
