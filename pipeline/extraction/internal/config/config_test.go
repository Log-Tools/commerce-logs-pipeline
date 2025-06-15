package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig_ValidConfig(t *testing.T) {
	configContent := `
kafka:
  brokers: "localhost:9092"
  input_topic: "Raw.ApplicationLogs"
  output_topic: "Extracted.Application"
  error_topic: "Extraction.Errors"
  consumer_group: "extraction-service"
  batch_size: 50
  flush_timeout_ms: 3000

processing:
  max_concurrency: 2
  enable_validation: true
  skip_invalid_logs: false
  log_parse_errors: true

logging:
  level: "debug"
  enable_metrics: true
  metrics_interval: 30
`

	tempFile := createTempConfigFile(t, configContent)
	defer os.Remove(tempFile)

	config, err := LoadConfig(tempFile)

	require.NoError(t, err)
	assert.Equal(t, "localhost:9092", config.Kafka.Brokers)
	assert.Equal(t, "Raw.ApplicationLogs", config.Kafka.InputTopic)
	assert.Equal(t, "Extracted.Application", config.Kafka.OutputTopic)
	assert.Equal(t, "Extraction.Errors", config.Kafka.ErrorTopic)
	assert.Equal(t, "extraction-service", config.Kafka.ConsumerGroup)
	assert.Equal(t, 50, config.Kafka.BatchSize)
	assert.Equal(t, 3000, config.Kafka.FlushTimeoutMs)
	assert.Equal(t, 2, config.Processing.MaxConcurrency)
	assert.True(t, config.Processing.EnableValidation)
	assert.False(t, config.Processing.SkipInvalidLogs)
	assert.True(t, config.Processing.LogParseErrors)
	assert.Equal(t, "debug", config.Logging.Level)
	assert.True(t, config.Logging.EnableMetrics)
	assert.Equal(t, 30, config.Logging.MetricsInterval)
}

func TestLoadConfig_WithDefaults(t *testing.T) {
	configContent := `
kafka:
  brokers: "localhost:9092"
  input_topic: "Raw.ApplicationLogs"
  output_topic: "Extracted.Application"
  error_topic: "Extraction.Errors"
  consumer_group: "extraction-service"

processing:
  max_concurrency: 2
`

	tempFile := createTempConfigFile(t, configContent)
	defer os.Remove(tempFile)

	config, err := LoadConfig(tempFile)

	require.NoError(t, err)
	// Check defaults are applied
	assert.Equal(t, 100, config.Kafka.BatchSize)         // default
	assert.Equal(t, 5000, config.Kafka.FlushTimeoutMs)   // default
	assert.Equal(t, 2, config.Processing.MaxConcurrency) // explicit value
	assert.Equal(t, "info", config.Logging.Level)        // default
	assert.Equal(t, 60, config.Logging.MetricsInterval)  // default
}

func TestLoadConfig_MissingRequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		wantErr string
	}{
		{
			name: "missing brokers",
			config: `
kafka:
  input_topic: "Raw.ApplicationLogs"
  output_topic: "Extracted.Application"
  consumer_group: "extraction-service"
`,
			wantErr: "kafka.brokers is required",
		},
		{
			name: "missing input topic",
			config: `
kafka:
  brokers: "localhost:9092"
  output_topic: "Extracted.Application"
  consumer_group: "extraction-service"
`,
			wantErr: "kafka.input_topic is required",
		},
		{
			name: "missing output topic",
			config: `
kafka:
  brokers: "localhost:9092"
  input_topic: "Raw.ApplicationLogs"
  consumer_group: "extraction-service"
`,
			wantErr: "kafka.output_topic is required",
		},
		{
			name: "missing consumer group",
			config: `
kafka:
  brokers: "localhost:9092"
  input_topic: "Raw.ApplicationLogs"
  output_topic: "Extracted.Application"
  error_topic: "Extraction.Errors"
`,
			wantErr: "kafka.consumer_group is required",
		},
		{
			name: "missing error topic",
			config: `
kafka:
  brokers: "localhost:9092"
  input_topic: "Raw.ApplicationLogs"
  output_topic: "Extracted.Application"
  consumer_group: "extraction-service"
`,
			wantErr: "kafka.error_topic is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempFile := createTempConfigFile(t, tt.config)
			defer os.Remove(tempFile)

			_, err := LoadConfig(tempFile)

			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestLoadConfig_InvalidLogLevel(t *testing.T) {
	configContent := `
kafka:
  brokers: "localhost:9092"
  input_topic: "Raw.ApplicationLogs"
  output_topic: "Extracted.Application"
  error_topic: "Extraction.Errors"
  consumer_group: "extraction-service"

processing:
  max_concurrency: 4

logging:
  level: "invalid"
`

	tempFile := createTempConfigFile(t, configContent)
	defer os.Remove(tempFile)

	_, err := LoadConfig(tempFile)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid log level 'invalid'")
}

func TestLoadConfig_InvalidConcurrency(t *testing.T) {
	configContent := `
kafka:
  brokers: "localhost:9092"
  input_topic: "Raw.ApplicationLogs"
  output_topic: "Extracted.Application"
  error_topic: "Extraction.Errors"
  consumer_group: "extraction-service"

processing:
  max_concurrency: 0
`

	tempFile := createTempConfigFile(t, configContent)
	defer os.Remove(tempFile)

	_, err := LoadConfig(tempFile)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "processing.max_concurrency must be at least 1")
}

func TestLoadConfig_FileNotFound(t *testing.T) {
	_, err := LoadConfig("nonexistent.yaml")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read config file")
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	configContent := `
kafka:
  brokers: "localhost:9092"
  input_topic: "Raw.ApplicationLogs"
  invalid yaml structure [
`

	tempFile := createTempConfigFile(t, configContent)
	defer os.Remove(tempFile)

	_, err := LoadConfig(tempFile)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse config YAML")
}

// Helper function to create temporary config files for testing
func createTempConfigFile(t *testing.T, content string) string {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "config.yaml")

	err := os.WriteFile(tempFile, []byte(content), 0644)
	require.NoError(t, err)

	return tempFile
}
