package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config represents the root configuration structure
type Config struct {
	Subscriptions map[string]Subscription `yaml:"subscriptions"`
	Kafka         KafkaConfig             `yaml:"kafka"`
}

// Subscription represents a subscription in the configuration
type Subscription struct {
	Name         string                 `yaml:"name"`
	Environments map[string]Environment `yaml:"environments"`
}

// Environment represents an environment within a subscription
type Environment struct {
	Name           string         `yaml:"name"`
	StorageAccount StorageAccount `yaml:"storage_account"`
}

// StorageAccount represents Azure Storage account configuration
type StorageAccount struct {
	AccountName string `yaml:"account_name"`
	AccessKey   string `yaml:"access_key"`
}

// KafkaConfig represents Kafka connection configuration
type KafkaConfig struct {
	Brokers      string            `yaml:"brokers"`
	ConsumerConf map[string]string `yaml:"consumer_config"`
}

// getConfigPaths returns possible config file locations, in order of preference
func getConfigPaths() []string {
	homeDir, _ := os.UserHomeDir()
	paths := []string{
		filepath.Join(homeDir, ".commerce-logs-pipeline", "config.yaml"),
		filepath.Join(homeDir, ".config", "commerce-logs-pipeline", "config.yaml"),
	}

	// Environment variable override
	if configPath := os.Getenv("COMMERCE_LOGS_CONFIG"); configPath != "" {
		paths = append([]string{configPath}, paths...)
	}

	// Local config (less secure, but convenient for dev)
	paths = append(paths, "config.yaml")
	paths = append(paths, "../../configs/config.yaml")

	return paths
}

// LoadConfig loads configuration from YAML file
func LoadConfig(configPath ...string) (*Config, error) {
	var paths []string
	if len(configPath) > 0 && configPath[0] != "" {
		paths = []string{configPath[0]}
	} else {
		paths = getConfigPaths()
	}

	for _, path := range paths {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			continue
		}

		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}

		var config Config
		if err := yaml.Unmarshal(data, &config); err != nil {
			continue
		}

		// Set default Kafka config if not provided
		if config.Kafka.Brokers == "" {
			config.Kafka.Brokers = "localhost:9092"
		}
		if config.Kafka.ConsumerConf == nil {
			config.Kafka.ConsumerConf = make(map[string]string)
		}
		if config.Kafka.ConsumerConf["auto.offset.reset"] == "" {
			config.Kafka.ConsumerConf["auto.offset.reset"] = "latest"
		}
		if config.Kafka.ConsumerConf["enable.auto.commit"] == "" {
			config.Kafka.ConsumerConf["enable.auto.commit"] = "true"
		}

		return &config, nil
	}

	return nil, fmt.Errorf("no valid configuration found in any of these locations: %s", strings.Join(paths, ", "))
}

// ListSubscriptions returns all available subscription IDs
func (c *Config) ListSubscriptions() []string {
	var subs []string
	for subID := range c.Subscriptions {
		subs = append(subs, subID)
	}
	return subs
}

// ListEnvironments returns all environments for a given subscription
func (c *Config) ListEnvironments(subscriptionID string) []string {
	sub, exists := c.Subscriptions[subscriptionID]
	if !exists {
		return nil
	}

	var envs []string
	for envID := range sub.Environments {
		envs = append(envs, envID)
	}
	return envs
}

// ValidateSubscriptionEnvironment validates that a subscription and environment exist
func (c *Config) ValidateSubscriptionEnvironment(subscription, environment string) error {
	sub, exists := c.Subscriptions[subscription]
	if !exists {
		return fmt.Errorf("subscription '%s' not found", subscription)
	}

	if environment != "" {
		_, exists := sub.Environments[environment]
		if !exists {
			return fmt.Errorf("environment '%s' not found in subscription '%s'", environment, subscription)
		}
	}

	return nil
}
