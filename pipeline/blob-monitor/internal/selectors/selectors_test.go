package selectors

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Validates all expected service selectors are properly registered and configured
func TestGetBlobSelectors(t *testing.T) {
	selectors := GetBlobSelectors()

	// Verify expected selectors exist
	expectedSelectors := []string{
		"apache-proxy",
		"api",
		"backoffice",
		"background-processing",
		"jsapps",
		"imageprocessing",
		"zookeeper",
	}

	for _, expected := range expectedSelectors {
		selector, exists := selectors[expected]
		assert.True(t, exists, "Selector '%s' should exist", expected)
		assert.NotNil(t, selector, "Selector '%s' should not be nil", expected)
		assert.Equal(t, expected, selector.Name, "Selector name should match key")
		assert.NotEmpty(t, selector.DisplayName, "Selector '%s' should have DisplayName", expected)
		assert.NotEmpty(t, selector.AzurePrefix, "Selector '%s' should have AzurePrefix", expected)
		assert.NotNil(t, selector.Predicate, "Selector '%s' should have Predicate function", expected)
	}

	assert.Equal(t, len(expectedSelectors), len(selectors), "Should have exact number of expected selectors")
}

// Verifies apache-proxy selector correctly identifies proxy blobs while excluding cache cleaners
func TestApacheProxySelector(t *testing.T) {
	selector, err := GetSelector("apache-proxy")
	require.NoError(t, err)

	tests := []struct {
		name     string
		blobName string
		expected bool
	}{
		{
			name:     "valid apache proxy blob",
			blobName: "kubernetes/20250607.apache2-igc_proxy-abc123.gz",
			expected: true,
		},
		{
			name:     "apache proxy without _proxy-",
			blobName: "kubernetes/20250607.apache2-igc-abc123.gz",
			expected: false,
		},
		{
			name:     "cache cleaner should be excluded",
			blobName: "kubernetes/20250607.apache2-igc_proxy-cache-cleaner-abc123.gz",
			expected: false,
		},
		{
			name:     "different service",
			blobName: "kubernetes/20250607.api-service-abc123.gz",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := selector.Predicate(tt.blobName)
			assert.Equal(t, tt.expected, result, "Predicate result for '%s'", tt.blobName)
		})
	}
}

// Verifies API selector accepts relevant blobs while excluding infrastructure components
func TestAPISelector(t *testing.T) {
	selector, err := GetSelector("api")
	require.NoError(t, err)

	tests := []struct {
		name     string
		blobName string
		expected bool
	}{
		{
			name:     "valid api blob",
			blobName: "kubernetes/20250607.api-service-abc123.gz",
			expected: true,
		},
		{
			name:     "cache cleaner should be excluded",
			blobName: "kubernetes/20250607.api-service_cache-cleaner-abc123.gz",
			expected: false,
		},
		{
			name:     "log forwarder should be excluded",
			blobName: "kubernetes/20250607.api-service_log-forwarder-abc123.gz",
			expected: false,
		},
		{
			name:     "different service",
			blobName: "kubernetes/20250607.zookeeper-abc123.gz",
			expected: true, // api selector accepts anything not explicitly excluded
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := selector.Predicate(tt.blobName)
			assert.Equal(t, tt.expected, result, "Predicate result for '%s'", tt.blobName)
		})
	}
}

// Verifies background-processing selector excludes infrastructure while accepting service logs
func TestBackgroundProcessingSelector(t *testing.T) {
	selector, err := GetSelector("background-processing")
	require.NoError(t, err)

	tests := []struct {
		name     string
		blobName string
		expected bool
	}{
		{
			name:     "valid background processing blob",
			blobName: "kubernetes/20250607.backgroundprocessing-service-abc123.gz",
			expected: true,
		},
		{
			name:     "cache cleaner should be excluded",
			blobName: "kubernetes/20250607.backgroundprocessing_cache-cleaner-abc123.gz",
			expected: false,
		},
		{
			name:     "log forwarder should be excluded",
			blobName: "kubernetes/20250607.backgroundprocessing_log-forwarder-abc123.gz",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := selector.Predicate(tt.blobName)
			assert.Equal(t, tt.expected, result, "Predicate result for '%s'", tt.blobName)
		})
	}
}

// Verifies zookeeper selector accepts all blobs due to no filtering requirements
func TestZookeeperSelector(t *testing.T) {
	selector, err := GetSelector("zookeeper")
	require.NoError(t, err)

	tests := []struct {
		name     string
		blobName string
		expected bool
	}{
		{
			name:     "valid zookeeper blob",
			blobName: "kubernetes/20250607.zookeeper-abc123.gz",
			expected: true,
		},
		{
			name:     "any zookeeper blob should match",
			blobName: "kubernetes/20250607.zookeeper-0_default_zookeeper-abc123.gz",
			expected: true,
		},
		{
			name:     "different service should also match (no filtering)",
			blobName: "kubernetes/20250607.other-service-abc123.gz",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := selector.Predicate(tt.blobName)
			assert.Equal(t, tt.expected, result, "Predicate result for '%s'", tt.blobName)
		})
	}
}

// Validates protection against configuration errors with unknown selector names
func TestValidateSelector(t *testing.T) {
	tests := []struct {
		name         string
		selectorName string
		expectError  bool
	}{
		{
			name:         "valid selector",
			selectorName: "apache-proxy",
			expectError:  false,
		},
		{
			name:         "unknown selector",
			selectorName: "unknown-selector",
			expectError:  true,
		},
		{
			name:         "empty selector name",
			selectorName: "",
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSelector(tt.selectorName)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Validates selector retrieval with proper error handling for missing selectors
func TestGetSelector(t *testing.T) {
	tests := []struct {
		name         string
		selectorName string
		expectError  bool
		expectNil    bool
	}{
		{
			name:         "valid selector",
			selectorName: "apache-proxy",
			expectError:  false,
			expectNil:    false,
		},
		{
			name:         "unknown selector",
			selectorName: "unknown",
			expectError:  true,
			expectNil:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selector, err := GetSelector(tt.selectorName)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			if tt.expectNil {
				assert.Nil(t, selector)
			} else {
				assert.NotNil(t, selector)
			}
		})
	}
}

// Validates date prefix generation for Azure blob listing API calls
func TestGetDatePrefix(t *testing.T) {
	selector, err := GetSelector("apache-proxy")
	require.NoError(t, err)

	tests := []struct {
		name     string
		date     string
		expected string
	}{
		{
			name:     "standard date format",
			date:     "20250607",
			expected: "kubernetes/20250607.apache2-igc",
		},
		{
			name:     "different date",
			date:     "20251225",
			expected: "kubernetes/20251225.apache2-igc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := selector.GetDatePrefix(tt.date)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Validates combined date and predicate filtering matches expected business logic
func TestFilterBlobsForDate(t *testing.T) {
	selector, err := GetSelector("apache-proxy")
	require.NoError(t, err)

	blobs := []string{
		"kubernetes/20250607.apache2-igc_proxy-valid.gz",
		"kubernetes/20250607.apache2-igc_proxy-cache-cleaner-invalid.gz", // Should be excluded
		"kubernetes/20250607.api-service-different.gz",                   // Wrong service
		"kubernetes/20250608.apache2-igc_proxy-wrong-date.gz",            // Wrong date
	}

	filtered := selector.FilterBlobsForDate(blobs, "20250607")

	expected := []string{
		"kubernetes/20250607.apache2-igc_proxy-valid.gz",
	}

	assert.Equal(t, expected, filtered)
}

// Ensures all selectors have required structural components for proper operation
func TestAllSelectorsHaveConsistentStructure(t *testing.T) {
	selectors := GetBlobSelectors()

	for name, selector := range selectors {
		t.Run(name, func(t *testing.T) {
			assert.NotEmpty(t, selector.Name, "Selector %s should have Name", name)
			assert.NotEmpty(t, selector.DisplayName, "Selector %s should have DisplayName", name)
			assert.NotEmpty(t, selector.Description, "Selector %s should have Description", name)
			assert.NotEmpty(t, selector.AzurePrefix, "Selector %s should have AzurePrefix", name)
			assert.NotEmpty(t, selector.ServicePrefix, "Selector %s should have ServicePrefix", name)
			assert.NotNil(t, selector.Predicate, "Selector %s should have Predicate function", name)

			// Test that predicate function works
			testBlob := "test-blob"
			result := selector.Predicate(testBlob)
			assert.IsType(t, true, result, "Predicate should return boolean for %s", name)
		})
	}
}

// Validates predicate functions behave consistently across different input variations
func TestSelectorPredicateConsistency(t *testing.T) {
	selectors := GetBlobSelectors()

	testInputs := []string{
		"",
		"short",
		"kubernetes/20250607.service-test.gz",
		"very-long-blob-name-with-multiple-components-and-separators",
		"blob_with_underscores_and-hyphens.gz",
	}

	for name, selector := range selectors {
		t.Run(name, func(t *testing.T) {
			for _, input := range testInputs {
				// Just ensure predicate doesn't panic and returns boolean
				result := selector.Predicate(input)
				assert.IsType(t, true, result, "Predicate for %s should return boolean for input: %s", name, input)
			}
		})
	}
}
