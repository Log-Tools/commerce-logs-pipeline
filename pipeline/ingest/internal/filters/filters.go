package filters

import (
	"crypto/md5"
	"fmt"
	"regexp"
	"time"

	"github.com/Log-Tools/commerce-logs-pipeline/ingest/internal/config"
)

// BlobInfo represents information about a blob for filtering
type BlobInfo struct {
	BlobName     string
	Subscription string
	Environment  string
	Selector     string
	LastModified time.Time
	State        string // "open" or "closed"
}

// BlobFilter provides filtering logic for blob processing
type BlobFilter struct {
	config   *config.FilterConfig
	compiled []*regexp.Regexp
}

// NewBlobFilter creates a new blob filter with compiled regex patterns
func NewBlobFilter(cfg *config.FilterConfig) (*BlobFilter, error) {
	var compiled []*regexp.Regexp

	// Compile selector regex patterns
	for _, pattern := range cfg.Selectors {
		regex, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid selector regex pattern %q: %w", pattern, err)
		}
		compiled = append(compiled, regex)
	}

	return &BlobFilter{
		config:   cfg,
		compiled: compiled,
	}, nil
}

// ShouldProcess determines if a blob should be processed based on all filters
func (f *BlobFilter) ShouldProcess(blobName, subscription, environment string, blobDate time.Time, shardingConfig *config.ShardingConfig) bool {
	// Apply date filter
	if !f.matchesDateFilter(blobDate) {
		return false
	}

	// Apply subscription filter
	if !f.matchesSubscriptionFilter(subscription) {
		return false
	}

	// Apply environment filter
	if !f.matchesEnvironmentFilter(environment) {
		return false
	}

	// Apply selector filter
	if !f.matchesSelectorFilter(blobName) {
		return false
	}

	// Apply sharding filter
	if !f.matchesShardingFilter(blobName, shardingConfig) {
		return false
	}

	return true
}

// matchesDateFilter checks if the blob date matches the date filter
func (f *BlobFilter) matchesDateFilter(blobDate time.Time) bool {
	// Check minimum date
	if f.config.MinDate != nil {
		minDate, err := time.Parse("2006-01-02", *f.config.MinDate)
		if err != nil {
			// Invalid date format, skip this filter
			return true
		}
		// Compare just the date part
		if blobDate.Format("2006-01-02") < minDate.Format("2006-01-02") {
			return false
		}
	}

	// Check maximum date
	if f.config.MaxDate != nil {
		maxDate, err := time.Parse("2006-01-02", *f.config.MaxDate)
		if err != nil {
			// Invalid date format, skip this filter
			return true
		}
		// Compare just the date part
		if blobDate.Format("2006-01-02") > maxDate.Format("2006-01-02") {
			return false
		}
	}

	return true
}

// matchesSubscriptionFilter checks if the subscription matches the filter
func (f *BlobFilter) matchesSubscriptionFilter(subscription string) bool {
	// Empty filter means match all
	if len(f.config.Subscriptions) == 0 {
		return true
	}

	// Check if subscription is in the allowed list
	for _, allowed := range f.config.Subscriptions {
		if subscription == allowed {
			return true
		}
	}

	return false
}

// matchesEnvironmentFilter checks if the environment matches the filter
func (f *BlobFilter) matchesEnvironmentFilter(environment string) bool {
	// Empty filter means match all
	if len(f.config.Environments) == 0 {
		return true
	}

	// Check if environment is in the allowed list
	for _, allowed := range f.config.Environments {
		if environment == allowed {
			return true
		}
	}

	return false
}

// matchesSelectorFilter checks if the blob name matches any selector regex
func (f *BlobFilter) matchesSelectorFilter(blobName string) bool {
	// Empty filter means match all
	if len(f.compiled) == 0 {
		return true
	}

	// Check if blob name matches any selector pattern
	for _, regex := range f.compiled {
		if regex.MatchString(blobName) {
			return true
		}
	}

	return false
}

// matchesShardingFilter checks if the blob should be processed by this shard
func (f *BlobFilter) matchesShardingFilter(blobName string, shardingConfig *config.ShardingConfig) bool {
	// If sharding is disabled, process all blobs
	if !shardingConfig.Enabled {
		return true
	}

	// Calculate hash of blob name
	hash := md5.Sum([]byte(blobName))
	hashValue := 0
	for _, b := range hash {
		hashValue += int(b)
	}

	// Check if this blob belongs to our shard
	return hashValue%shardingConfig.ShardsCount == shardingConfig.ShardNumber
}

// GetShardForBlob returns the shard number for a given blob name
func GetShardForBlob(blobName string, shardingConfig *config.ShardingConfig) int {
	if !shardingConfig.Enabled {
		return 0
	}

	hash := md5.Sum([]byte(blobName))
	hashValue := 0
	for _, b := range hash {
		hashValue += int(b)
	}
	return hashValue % shardingConfig.ShardsCount
}
