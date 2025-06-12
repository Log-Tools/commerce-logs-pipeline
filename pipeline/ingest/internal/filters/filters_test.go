package filters

import (
	"fmt"
	"testing"
	"time"

	"github.com/Log-Tools/commerce-logs-pipeline/ingest/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBlobFilter(t *testing.T) {
	t.Run("creates filter with valid config", func(t *testing.T) {
		cfg := &config.FilterConfig{
			MinDate:       stringPtr("2024-01-01"),
			MaxDate:       stringPtr("2024-12-31"),
			Subscriptions: []string{"sub1", "sub2"},
			Environments:  []string{"prod", "staging"},
			Selectors:     []string{"api-.*", "web-.*"},
		}

		filter, err := NewBlobFilter(cfg)
		require.NoError(t, err)
		assert.NotNil(t, filter)
	})

	t.Run("handles invalid regex in selectors", func(t *testing.T) {
		cfg := &config.FilterConfig{
			Selectors: []string{"[invalid-regex"},
		}

		filter, err := NewBlobFilter(cfg)
		assert.Error(t, err)
		assert.Nil(t, filter)
	})

	t.Run("handles invalid date format during processing", func(t *testing.T) {
		cfg := &config.FilterConfig{
			MinDate: stringPtr("invalid-date"),
		}

		// Filter creation should succeed (date validation happens during processing)
		filter, err := NewBlobFilter(cfg)
		assert.NoError(t, err)
		assert.NotNil(t, filter)

		// But it should gracefully handle invalid dates during processing
		// by accepting all blobs (as seen in the implementation)
		result := filter.ShouldProcess("test.gz", "sub1", "env1", time.Now(), &config.ShardingConfig{Enabled: false})
		assert.True(t, result, "Should accept blob when date format is invalid")
	})
}

func TestBlobFilter_ShouldProcess(t *testing.T) {
	// Test date for blob processing
	testDate := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		name         string
		config       *config.FilterConfig
		blobName     string
		subscription string
		environment  string
		lastModified time.Time
		sharding     *config.ShardingConfig
		expected     bool
	}{
		{
			name:         "no filters - accepts all",
			config:       &config.FilterConfig{},
			blobName:     "test-blob.gz",
			subscription: "sub1",
			environment:  "prod",
			lastModified: testDate,
			sharding:     &config.ShardingConfig{Enabled: false},
			expected:     true,
		},
		{
			name: "date filter - within range",
			config: &config.FilterConfig{
				MinDate: stringPtr("2024-01-01"),
				MaxDate: stringPtr("2024-12-31"),
			},
			blobName:     "test-blob.gz",
			subscription: "sub1",
			environment:  "prod",
			lastModified: testDate,
			sharding:     &config.ShardingConfig{Enabled: false},
			expected:     true,
		},
		{
			name: "date filter - before min date",
			config: &config.FilterConfig{
				MinDate: stringPtr("2024-07-01"),
			},
			blobName:     "test-blob.gz",
			subscription: "sub1",
			environment:  "prod",
			lastModified: testDate,
			sharding:     &config.ShardingConfig{Enabled: false},
			expected:     false,
		},
		{
			name: "date filter - after max date",
			config: &config.FilterConfig{
				MaxDate: stringPtr("2024-05-01"),
			},
			blobName:     "test-blob.gz",
			subscription: "sub1",
			environment:  "prod",
			lastModified: testDate,
			sharding:     &config.ShardingConfig{Enabled: false},
			expected:     false,
		},
		{
			name: "subscription filter - matches",
			config: &config.FilterConfig{
				Subscriptions: []string{"sub1", "sub2"},
			},
			blobName:     "test-blob.gz",
			subscription: "sub1",
			environment:  "prod",
			lastModified: testDate,
			sharding:     &config.ShardingConfig{Enabled: false},
			expected:     true,
		},
		{
			name: "subscription filter - no match",
			config: &config.FilterConfig{
				Subscriptions: []string{"sub2", "sub3"},
			},
			blobName:     "test-blob.gz",
			subscription: "sub1",
			environment:  "prod",
			lastModified: testDate,
			sharding:     &config.ShardingConfig{Enabled: false},
			expected:     false,
		},
		{
			name: "environment filter - matches",
			config: &config.FilterConfig{
				Environments: []string{"prod", "staging"},
			},
			blobName:     "test-blob.gz",
			subscription: "sub1",
			environment:  "prod",
			lastModified: testDate,
			sharding:     &config.ShardingConfig{Enabled: false},
			expected:     true,
		},
		{
			name: "environment filter - no match",
			config: &config.FilterConfig{
				Environments: []string{"staging", "dev"},
			},
			blobName:     "test-blob.gz",
			subscription: "sub1",
			environment:  "prod",
			lastModified: testDate,
			sharding:     &config.ShardingConfig{Enabled: false},
			expected:     false,
		},
		{
			name: "selector filter - matches regex",
			config: &config.FilterConfig{
				Selectors: []string{"api-.*", "web-.*"},
			},
			blobName:     "api-service-20240615.gz",
			subscription: "sub1",
			environment:  "prod",
			lastModified: testDate,
			sharding:     &config.ShardingConfig{Enabled: false},
			expected:     true,
		},
		{
			name: "selector filter - no match",
			config: &config.FilterConfig{
				Selectors: []string{"api-.*", "web-.*"},
			},
			blobName:     "database-backup-20240615.gz",
			subscription: "sub1",
			environment:  "prod",
			lastModified: testDate,
			sharding:     &config.ShardingConfig{Enabled: false},
			expected:     false,
		},
		{
			name: "multiple filters - all match",
			config: &config.FilterConfig{
				MinDate:       stringPtr("2024-01-01"),
				MaxDate:       stringPtr("2024-12-31"),
				Subscriptions: []string{"sub1"},
				Environments:  []string{"prod"},
				Selectors:     []string{"api-.*"},
			},
			blobName:     "api-service-20240615.gz",
			subscription: "sub1",
			environment:  "prod",
			lastModified: testDate,
			sharding:     &config.ShardingConfig{Enabled: false},
			expected:     true,
		},
		{
			name: "multiple filters - one fails",
			config: &config.FilterConfig{
				MinDate:       stringPtr("2024-01-01"),
				MaxDate:       stringPtr("2024-12-31"),
				Subscriptions: []string{"sub1"},
				Environments:  []string{"staging"}, // Different environment
				Selectors:     []string{"api-.*"},
			},
			blobName:     "api-service-20240615.gz",
			subscription: "sub1",
			environment:  "prod",
			lastModified: testDate,
			sharding:     &config.ShardingConfig{Enabled: false},
			expected:     false,
		},
		{
			name:         "sharding enabled - blob assigned to this shard",
			config:       &config.FilterConfig{},
			blobName:     "shard-test-blob.gz",
			subscription: "sub1",
			environment:  "prod",
			lastModified: testDate,
			sharding: &config.ShardingConfig{
				Enabled:     true,
				ShardsCount: 3,
				ShardNumber: GetShardForBlob("shard-test-blob.gz", &config.ShardingConfig{Enabled: true, ShardsCount: 3}),
			},
			expected: true,
		},
		{
			name:         "sharding enabled - blob assigned to different shard",
			config:       &config.FilterConfig{},
			blobName:     "shard-test-blob.gz",
			subscription: "sub1",
			environment:  "prod",
			lastModified: testDate,
			sharding: &config.ShardingConfig{
				Enabled:     true,
				ShardsCount: 3,
				ShardNumber: (GetShardForBlob("shard-test-blob.gz", &config.ShardingConfig{Enabled: true, ShardsCount: 3}) + 1) % 3, // Different shard
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewBlobFilter(tt.config)
			require.NoError(t, err)

			result := filter.ShouldProcess(
				tt.blobName,
				tt.subscription,
				tt.environment,
				tt.lastModified,
				tt.sharding,
			)

			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBlobFilter_DateFiltering(t *testing.T) {
	// Test specific date scenarios
	t.Run("handles edge cases for date filtering", func(t *testing.T) {
		cfg := &config.FilterConfig{
			MinDate: stringPtr("2024-06-15"),
			MaxDate: stringPtr("2024-06-15"),
		}

		filter, err := NewBlobFilter(cfg)
		require.NoError(t, err)

		// Exact date match
		exactDate := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
		result := filter.ShouldProcess("test.gz", "sub1", "env1", exactDate, &config.ShardingConfig{Enabled: false})
		assert.True(t, result)

		// Different time same date
		sameDay := time.Date(2024, 6, 15, 23, 59, 59, 0, time.UTC)
		result = filter.ShouldProcess("test.gz", "sub1", "env1", sameDay, &config.ShardingConfig{Enabled: false})
		assert.True(t, result)

		// Day before
		dayBefore := time.Date(2024, 6, 14, 23, 59, 59, 0, time.UTC)
		result = filter.ShouldProcess("test.gz", "sub1", "env1", dayBefore, &config.ShardingConfig{Enabled: false})
		assert.False(t, result)

		// Day after
		dayAfter := time.Date(2024, 6, 16, 0, 0, 1, 0, time.UTC)
		result = filter.ShouldProcess("test.gz", "sub1", "env1", dayAfter, &config.ShardingConfig{Enabled: false})
		assert.False(t, result)
	})
}

func TestBlobFilter_RegexFiltering(t *testing.T) {
	t.Run("handles complex regex patterns", func(t *testing.T) {
		cfg := &config.FilterConfig{
			Selectors: []string{
				`^api-.*\.gz$`,            // Must start with "api-" and end with ".gz"
				`.*proxy.*`,               // Contains "proxy"
				`^web-\d{8}-.*\.log\.gz$`, // web-YYYYMMDD-*.log.gz format
			},
		}

		filter, err := NewBlobFilter(cfg)
		require.NoError(t, err)

		testCases := []struct {
			blobName string
			expected bool
		}{
			{"api-service.gz", true},
			{"api-service.txt", false},
			{"not-api-service.gz", false},
			{"proxy-logs.txt", true},
			{"apache-proxy-access.log", true},
			{"web-20240615-access.log.gz", true},
			{"web-invalid-date.log.gz", false},
			{"api-20240615.log.gz", true}, // Matches first pattern
		}

		for _, tc := range testCases {
			result := filter.ShouldProcess(
				tc.blobName,
				"sub1",
				"env1",
				time.Now(),
				&config.ShardingConfig{Enabled: false},
			)
			assert.Equal(t, tc.expected, result, "Failed for blob: %s", tc.blobName)
		}
	})
}

func TestShardAssignment(t *testing.T) {
	shardingConfig := &config.ShardingConfig{
		Enabled:     true,
		ShardsCount: 5,
	}

	t.Run("consistent shard assignment", func(t *testing.T) {
		blobName := "test-consistent-shard.gz"

		// Same blob should always be assigned to same shard
		shard1 := GetShardForBlob(blobName, shardingConfig)
		shard2 := GetShardForBlob(blobName, shardingConfig)
		assert.Equal(t, shard1, shard2)
	})

	t.Run("distributes blobs across shards", func(t *testing.T) {
		shardCounts := make(map[int]int)
		totalShards := 5

		// Test with many blob names to check distribution
		for i := 0; i < 100; i++ {
			blobName := fmt.Sprintf("test-blob-%d.gz", i)
			shard := GetShardForBlob(blobName, &config.ShardingConfig{
				Enabled:     true,
				ShardsCount: totalShards,
			})
			shardCounts[shard]++
		}

		// Each shard should have some blobs (basic distribution check)
		for i := 0; i < totalShards; i++ {
			assert.Greater(t, shardCounts[i], 0, "Shard %d has no blobs assigned", i)
		}
	})
}

// Helper functions
func stringPtr(s string) *string {
	return &s
}
