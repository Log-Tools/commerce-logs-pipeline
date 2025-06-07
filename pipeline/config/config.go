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

// SafeStorageAccount represents a storage account with masked credentials for safe display
type SafeStorageAccount struct {
	AccountName string `yaml:"account_name"`
	AccessKey   string `yaml:"access_key"` // This will be masked
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

		return &config, nil
	}

	return nil, fmt.Errorf("no valid configuration found in any of these locations: %s", strings.Join(paths[:len(paths)-1], ", "))
}

// HasCredentials checks if credentials are configured
func HasCredentials() bool {
	_, err := LoadConfig()
	return err == nil
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

// GetStorageAccount returns storage account configuration for a specific subscription and environment
func (c *Config) GetStorageAccount(subscriptionID, environment string) (StorageAccount, error) {
	sub, exists := c.Subscriptions[subscriptionID]
	if !exists {
		return StorageAccount{}, fmt.Errorf("subscription '%s' not found", subscriptionID)
	}

	env, exists := sub.Environments[environment]
	if !exists {
		return StorageAccount{}, fmt.Errorf("environment '%s' not found in subscription '%s'", environment, subscriptionID)
	}

	return env.StorageAccount, nil
}

// GetStorageAccountSafe returns storage account configuration with masked credentials
func (c *Config) GetStorageAccountSafe(subscriptionID, environment string) (SafeStorageAccount, error) {
	storage, err := c.GetStorageAccount(subscriptionID, environment)
	if err != nil {
		return SafeStorageAccount{}, err
	}

	return SafeStorageAccount{
		AccountName: storage.AccountName,
		AccessKey:   maskCredential(storage.AccessKey, 4),
	}, nil
}

// maskCredential masks a credential keeping only the first few characters visible
func maskCredential(value string, visibleChars int) string {
	if len(value) <= visibleChars {
		return strings.Repeat("*", 8)
	}
	return value[:visibleChars] + strings.Repeat("*", len(value)-visibleChars)
}

// ValidateConfig validates the configuration structure
func (c *Config) ValidateConfig() (bool, []string) {
	var issues []string

	if len(c.Subscriptions) == 0 {
		issues = append(issues, "no subscriptions found")
		return false, issues
	}

	for subID, sub := range c.Subscriptions {
		if len(sub.Environments) == 0 {
			issues = append(issues, fmt.Sprintf("subscription '%s' has no environments", subID))
			continue
		}

		for envID, env := range sub.Environments {
			if env.StorageAccount.AccountName == "" {
				issues = append(issues, fmt.Sprintf("%s/%s missing 'account_name'", subID, envID))
			}
			if env.StorageAccount.AccessKey == "" {
				issues = append(issues, fmt.Sprintf("%s/%s missing 'access_key'", subID, envID))
			}
		}
	}

	return len(issues) == 0, issues
}
