package selectors

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseKubernetesBlobName tests the blob name parser with real production examples
func TestParseKubernetesBlobName(t *testing.T) {
	tests := []struct {
		name     string
		blobName string
		expected KubernetesBlobName
	}{
		{
			name:     "apache proxy blob",
			blobName: "20250613.apache2-igc-9db94ff4f-b6w9z_default_proxy-54353eeb68e263ecb7adf59a989cd5eb7f3ba0545d8c63ad2c5a5eb7ff6f106b.gz",
			expected: KubernetesBlobName{
				Date:        "20250613",
				PodName:     "apache2-igc-9db94ff4f-b6w9z",
				Namespace:   "default",
				Container:   "proxy",
				ContainerID: "54353eeb68e263ecb7adf59a989cd5eb7f3ba0545d8c63ad2c5a5eb7ff6f106b",
				Valid:       true,
			},
		},
		{
			name:     "apache nat proxy blob",
			blobName: "20250613.apache2-igc-nat-7c58b477db-42fg8_default_proxy-9dc8fce39dc3815ad7bf878d4c08d2556cf816f891acdb63e2fb7c5f5295acef.gz",
			expected: KubernetesBlobName{
				Date:        "20250613",
				PodName:     "apache2-igc-nat-7c58b477db-42fg8",
				Namespace:   "default",
				Container:   "proxy",
				ContainerID: "9dc8fce39dc3815ad7bf878d4c08d2556cf816f891acdb63e2fb7c5f5295acef",
				Valid:       true,
			},
		},
		{
			name:     "api platform blob",
			blobName: "20250613.api-d64987b96-ctnd7_default_platform-c8c329006cf3420e14b2eb0bfa7d16f793cab8cae4347f26c84f5800aadfec70.gz",
			expected: KubernetesBlobName{
				Date:        "20250613",
				PodName:     "api-d64987b96-ctnd7",
				Namespace:   "default",
				Container:   "platform",
				ContainerID: "c8c329006cf3420e14b2eb0bfa7d16f793cab8cae4347f26c84f5800aadfec70",
				Valid:       true,
			},
		},
		{
			name:     "jsapps blob",
			blobName: "20250613.jsapps-6cb865fc5b-bgg2c_default_jsapps-fbc78162b8d76145f882a267bff7150838219a985402be6d989e2181036fed5b.gz",
			expected: KubernetesBlobName{
				Date:        "20250613",
				PodName:     "jsapps-6cb865fc5b-bgg2c",
				Namespace:   "default",
				Container:   "jsapps",
				ContainerID: "fbc78162b8d76145f882a267bff7150838219a985402be6d989e2181036fed5b",
				Valid:       true,
			},
		},
		{
			name:     "zookeeper stateful set blob",
			blobName: "20250613.zookeeper-1_default_zookeeper-1b70963310b2bf2a1e26f5dbed4f6d8d2ae883e1c2660c1549ad66dad2fed54d.gz",
			expected: KubernetesBlobName{
				Date:        "20250613",
				PodName:     "zookeeper-1",
				Namespace:   "default",
				Container:   "zookeeper",
				ContainerID: "1b70963310b2bf2a1e26f5dbed4f6d8d2ae883e1c2660c1549ad66dad2fed54d",
				Valid:       true,
			},
		},
		{
			name:     "imageprocessing blob",
			blobName: "20250613.imageprocessing-64bd7f67f4-t8bqn_default_imageprocessing-9b714e7d980f0676baaa74d7575edda259dfa04fcf3a2067094bcbd42273cd06.gz",
			expected: KubernetesBlobName{
				Date:        "20250613",
				PodName:     "imageprocessing-64bd7f67f4-t8bqn",
				Namespace:   "default",
				Container:   "imageprocessing",
				ContainerID: "9b714e7d980f0676baaa74d7575edda259dfa04fcf3a2067094bcbd42273cd06",
				Valid:       true,
			},
		},
		{
			name:     "solr stateful set blob",
			blobName: "20250613.solr-0_default_solr-4a18b5b3ae06c609dbe08ceb94cc8cdc189d7e7ff0b973fcbf82b8e9aed9a81b.gz",
			expected: KubernetesBlobName{
				Date:        "20250613",
				PodName:     "solr-0",
				Namespace:   "default",
				Container:   "solr",
				ContainerID: "4a18b5b3ae06c609dbe08ceb94cc8cdc189d7e7ff0b973fcbf82b8e9aed9a81b",
				Valid:       true,
			},
		},
		{
			name:     "hybris autoscaler blob",
			blobName: "20250613.hybris-autoscaler-7cb9764759-vrhrc_default_hybris-autoscaler-7592928f90a158e281b832fd79acd146c42251c81712704582ecc0e18df8e982.gz",
			expected: KubernetesBlobName{
				Date:        "20250613",
				PodName:     "hybris-autoscaler-7cb9764759-vrhrc",
				Namespace:   "default",
				Container:   "hybris-autoscaler",
				ContainerID: "7592928f90a158e281b832fd79acd146c42251c81712704582ecc0e18df8e982",
				Valid:       true,
			},
		},
		{
			name:     "invalid - no .gz extension",
			blobName: "20250613.apache2-igc-9db94ff4f-b6w9z_default_proxy-54353eeb68e263ecb7adf59a989cd5eb7f3ba0545d8c63ad2c5a5eb7ff6f106b",
			expected: KubernetesBlobName{Valid: false},
		},
		{
			name:     "invalid - no dot separator",
			blobName: "20250613apache2-igc-9db94ff4f-b6w9z_default_proxy-54353eeb68e263ecb7adf59a989cd5eb7f3ba0545d8c63ad2c5a5eb7ff6f106b.gz",
			expected: KubernetesBlobName{Valid: false},
		},
		{
			name:     "invalid - no underscore separator",
			blobName: "20250613.apache2-igc-9db94ff4f-b6w9zdefaultproxy-54353eeb68e263ecb7adf59a989cd5eb7f3ba0545d8c63ad2c5a5eb7ff6f106b.gz",
			expected: KubernetesBlobName{
				Date:  "20250613",
				Valid: false,
			},
		},
		{
			name:     "invalid - no dash in container part",
			blobName: "20250613.apache2-igc-9db94ff4f-b6w9z_default_proxy54353eeb68e263ecb7adf59a989cd5eb7f3ba0545d8c63ad2c5a5eb7ff6f106b.gz",
			expected: KubernetesBlobName{
				Date:    "20250613",
				PodName: "apache2-igc-9db94ff4f-b6w9z",
				Valid:   false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseKubernetesBlobName(tt.blobName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

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

// Verifies apache-proxy selector correctly identifies proxy blobs while excluding NAT controllers
func TestApacheProxySelector(t *testing.T) {
	selector, err := GetSelector("apache-proxy")
	require.NoError(t, err)

	tests := []struct {
		name     string
		blobName string
		expected bool
		reason   string
	}{
		{
			name:     "valid apache proxy blob from D1",
			blobName: "20250613.apache2-igc-9db94ff4f-b6w9z_default_proxy-54353eeb68e263ecb7adf59a989cd5eb7f3ba0545d8c63ad2c5a5eb7ff6f106b.gz",
			expected: true,
			reason:   "Standard apache proxy pod with HTTP traffic logs",
		},
		{
			name:     "valid apache proxy blob from S1",
			blobName: "20250613.apache2-igc-6975454b68-bqgcf_default_proxy-ffb80463cddd21c56e8044ec033766da598ea347d5f4d2151931b4366155dd5f.gz",
			expected: true,
			reason:   "Standard apache proxy pod with HTTP traffic logs",
		},
		{
			name:     "valid apache proxy blob from P1",
			blobName: "20250613.apache2-igc-fc65bcd5-r99kz_default_proxy-7139cdf497d345abc7ef0df7b1deffba5429c5f0c2610395d4ba7e815daae6fa.gz",
			expected: true,
			reason:   "Standard apache proxy pod with HTTP traffic logs",
		},
		{
			name:     "apache NAT controller should be excluded",
			blobName: "20250613.apache2-igc-nat-7c58b477db-42fg8_default_proxy-9dc8fce39dc3815ad7bf878d4c08d2556cf816f891acdb63e2fb7c5f5295acef.gz",
			expected: false,
			reason:   "NAT pods contain only ingress controller logs, not HTTP traffic",
		},
		{
			name:     "another apache NAT controller should be excluded",
			blobName: "20250613.apache2-igc-nat-58478675b5-2rrts_default_proxy-54ca8d2b28200fd24008dd4bd49e6b5e883ca221c9fb658497c861ef9fa77237a.gz",
			expected: false,
			reason:   "NAT pods contain only ingress controller logs, not HTTP traffic",
		},
		{
			name:     "API service should be excluded",
			blobName: "20250613.api-d64987b96-ctnd7_default_platform-c8c329006cf3420e14b2eb0bfa7d16f793cab8cae4347f26c84f5800aadfec70.gz",
			expected: false,
			reason:   "Different service (API), not apache proxy",
		},
		{
			name:     "wrong namespace should be excluded",
			blobName: "20250613.apache2-igc-9db94ff4f-b6w9z_kube-system_proxy-54353eeb68e263ecb7adf59a989cd5eb7f3ba0545d8c63ad2c5a5eb7ff6f106b.gz",
			expected: false,
			reason:   "Wrong namespace (kube-system instead of default)",
		},
		{
			name:     "wrong container should be excluded",
			blobName: "20250613.apache2-igc-9db94ff4f-b6w9z_default_sidecar-54353eeb68e263ecb7adf59a989cd5eb7f3ba0545d8c63ad2c5a5eb7ff6f106b.gz",
			expected: false,
			reason:   "Wrong container (sidecar instead of proxy)",
		},
		{
			name:     "invalid blob structure should be excluded",
			blobName: "invalid-blob-name.gz",
			expected: false,
			reason:   "Invalid blob name structure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := selector.Predicate(tt.blobName)
			assert.Equal(t, tt.expected, result, "Predicate result for '%s': %s", tt.blobName, tt.reason)
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
		reason   string
	}{
		{
			name:     "valid API blob from D1",
			blobName: "20250613.api-d64987b96-ctnd7_default_platform-c8c329006cf3420e14b2eb0bfa7d16f793cab8cae4347f26c84f5800aadfec70.gz",
			expected: true,
			reason:   "Standard API service platform container",
		},
		{
			name:     "valid API blob from P1",
			blobName: "20250613.api-59ff9cc6f9-2n78p_default_platform-1e2655e19ae10d4b31d1a37aa1e72df4846a5e8bb3d2d27c2a15d02942f6e551.gz",
			expected: true,
			reason:   "Standard API service platform container",
		},
		{
			name:     "cache cleaner should be excluded",
			blobName: "20250613.api-d64987b96-ctnd7_default_cache-cleaner-c8c329006cf3420e14b2eb0bfa7d16f793cab8cae4347f26c84f5800aadfec70.gz",
			expected: false,
			reason:   "Cache cleaner is infrastructure, not business logic",
		},
		{
			name:     "log forwarder should be excluded",
			blobName: "20250613.api-d64987b96-ctnd7_default_log-forwarder-c8c329006cf3420e14b2eb0bfa7d16f793cab8cae4347f26c84f5800aadfec70.gz",
			expected: false,
			reason:   "Log forwarder is infrastructure, not business logic",
		},
		{
			name:     "install-oneagent should be excluded",
			blobName: "20250613.api-d64987b96-ctnd7_default_install-oneagent-414f37aa2bb131c34f3efc074f1549fb5283d831882676ff2ed95a197b015223.gz",
			expected: false,
			reason:   "Install-oneagent is a sidecar container, not the main API service",
		},
		{
			name:     "different service should be excluded",
			blobName: "20250613.apache2-igc-9db94ff4f-b6w9z_default_proxy-54353eeb68e263ecb7adf59a989cd5eb7f3ba0545d8c63ad2c5a5eb7ff6f106b.gz",
			expected: false,
			reason:   "Different service (apache), not API",
		},
		{
			name:     "invalid blob structure should be excluded",
			blobName: "invalid-blob-name.gz",
			expected: false,
			reason:   "Invalid blob name structure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := selector.Predicate(tt.blobName)
			assert.Equal(t, tt.expected, result, "Predicate result for '%s': %s", tt.blobName, tt.reason)
		})
	}
}

// Verifies backoffice selector excludes infrastructure while accepting service logs
func TestBackofficeSelector(t *testing.T) {
	selector, err := GetSelector("backoffice")
	require.NoError(t, err)

	tests := []struct {
		name     string
		blobName string
		expected bool
		reason   string
	}{
		{
			name:     "valid backoffice blob",
			blobName: "20250613.backoffice-677f87b859-x9xn8_default_platform-6cf0923e99581a6ae97bb9306473a5b19acb403aa611ebced20f22ba1d19312c.gz",
			expected: true,
			reason:   "Standard backoffice service platform container",
		},
		{
			name:     "cache cleaner should be excluded",
			blobName: "20250613.backoffice-677f87b859-x9xn8_default_cache-cleaner-6cf0923e99581a6ae97bb9306473a5b19acb403aa611ebced20f22ba1d19312c.gz",
			expected: false,
			reason:   "Cache cleaner is infrastructure, not business logic",
		},
		{
			name:     "different service should be excluded",
			blobName: "20250613.api-d64987b96-ctnd7_default_platform-c8c329006cf3420e14b2eb0bfa7d16f793cab8cae4347f26c84f5800aadfec70.gz",
			expected: false,
			reason:   "Different service (API), not backoffice",
		},
		{
			name:     "invalid blob structure should be excluded",
			blobName: "invalid-blob-name.gz",
			expected: false,
			reason:   "Invalid blob name structure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := selector.Predicate(tt.blobName)
			assert.Equal(t, tt.expected, result, "Predicate result for '%s': %s", tt.blobName, tt.reason)
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
		reason   string
	}{
		{
			name:     "valid background processing blob",
			blobName: "20250613.backgroundprocessing-7f56bcb6f-h6j87_default_platform-553342aff8970de7d26fee63023126579c63b68a0730421f334d886cdc213349.gz",
			expected: true,
			reason:   "Standard background processing service platform container",
		},
		{
			name:     "cache cleaner should be excluded",
			blobName: "20250613.backgroundprocessing-7f56bcb6f-h6j87_default_cache-cleaner-553342aff8970de7d26fee63023126579c63b68a0730421f334d886cdc213349.gz",
			expected: false,
			reason:   "Cache cleaner is infrastructure, not business logic",
		},
		{
			name:     "log forwarder should be excluded",
			blobName: "20250613.backgroundprocessing-7f56bcb6f-h6j87_default_log-forwarder-553342aff8970de7d26fee63023126579c63b68a0730421f334d886cdc213349.gz",
			expected: false,
			reason:   "Log forwarder is infrastructure, not business logic",
		},
		{
			name:     "different service should be excluded",
			blobName: "20250613.api-d64987b96-ctnd7_default_platform-c8c329006cf3420e14b2eb0bfa7d16f793cab8cae4347f26c84f5800aadfec70.gz",
			expected: false,
			reason:   "Different service (API), not background processing",
		},
		{
			name:     "invalid blob structure should be excluded",
			blobName: "invalid-blob-name.gz",
			expected: false,
			reason:   "Invalid blob name structure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := selector.Predicate(tt.blobName)
			assert.Equal(t, tt.expected, result, "Predicate result for '%s': %s", tt.blobName, tt.reason)
		})
	}
}

// Verifies jsapps selector correctly identifies jsapps containers
func TestJsappsSelector(t *testing.T) {
	selector, err := GetSelector("jsapps")
	require.NoError(t, err)

	tests := []struct {
		name     string
		blobName string
		expected bool
		reason   string
	}{
		{
			name:     "valid jsapps blob",
			blobName: "20250613.jsapps-6cb865fc5b-bgg2c_default_jsapps-fbc78162b8d76145f882a267bff7150838219a985402be6d989e2181036fed5b.gz",
			expected: true,
			reason:   "Standard jsapps service container",
		},
		{
			name:     "cache cleaner should be excluded",
			blobName: "20250613.jsapps-6cb865fc5b-bgg2c_default_cache-cleaner-fbc78162b8d76145f882a267bff7150838219a985402be6d989e2181036fed5b.gz",
			expected: false,
			reason:   "Cache cleaner is infrastructure, not business logic",
		},
		{
			name:     "different service should be excluded",
			blobName: "20250613.api-d64987b96-ctnd7_default_platform-c8c329006cf3420e14b2eb0bfa7d16f793cab8cae4347f26c84f5800aadfec70.gz",
			expected: false,
			reason:   "Different service (API), not jsapps",
		},
		{
			name:     "invalid blob structure should be excluded",
			blobName: "invalid-blob-name.gz",
			expected: false,
			reason:   "Invalid blob name structure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := selector.Predicate(tt.blobName)
			assert.Equal(t, tt.expected, result, "Predicate result for '%s': %s", tt.blobName, tt.reason)
		})
	}
}

// Verifies imageprocessing selector correctly identifies imageprocessing containers
func TestImageprocessingSelector(t *testing.T) {
	selector, err := GetSelector("imageprocessing")
	require.NoError(t, err)

	tests := []struct {
		name     string
		blobName string
		expected bool
		reason   string
	}{
		{
			name:     "valid imageprocessing blob",
			blobName: "20250613.imageprocessing-64bd7f67f4-t8bqn_default_imageprocessing-9b714e7d980f0676baaa74d7575edda259dfa04fcf3a2067094bcbd42273cd06.gz",
			expected: true,
			reason:   "Standard imageprocessing service container",
		},
		{
			name:     "cache cleaner should be excluded",
			blobName: "20250613.imageprocessing-64bd7f67f4-t8bqn_default_cache-cleaner-9b714e7d980f0676baaa74d7575edda259dfa04fcf3a2067094bcbd42273cd06.gz",
			expected: false,
			reason:   "Cache cleaner is infrastructure, not business logic",
		},
		{
			name:     "different service should be excluded",
			blobName: "20250613.api-d64987b96-ctnd7_default_platform-c8c329006cf3420e14b2eb0bfa7d16f793cab8cae4347f26c84f5800aadfec70.gz",
			expected: false,
			reason:   "Different service (API), not imageprocessing",
		},
		{
			name:     "invalid blob structure should be excluded",
			blobName: "invalid-blob-name.gz",
			expected: false,
			reason:   "Invalid blob name structure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := selector.Predicate(tt.blobName)
			assert.Equal(t, tt.expected, result, "Predicate result for '%s': %s", tt.blobName, tt.reason)
		})
	}
}

// Verifies zookeeper selector accepts all zookeeper blobs (no filtering needed)
func TestZookeeperSelector(t *testing.T) {
	selector, err := GetSelector("zookeeper")
	require.NoError(t, err)

	tests := []struct {
		name     string
		blobName string
		expected bool
		reason   string
	}{
		{
			name:     "valid zookeeper-0 blob",
			blobName: "20250613.zookeeper-0_default_zookeeper-1fe03bc52c45916eca99bb8300153d17e36961e43d2b1537042deffec1fa75bd.gz",
			expected: true,
			reason:   "Zookeeper stateful set pod 0",
		},
		{
			name:     "valid zookeeper-1 blob",
			blobName: "20250613.zookeeper-1_default_zookeeper-1b70963310b2bf2a1e26f5dbed4f6d8d2ae883e1c2660c1549ad66dad2fed54d.gz",
			expected: true,
			reason:   "Zookeeper stateful set pod 1",
		},
		{
			name:     "valid zookeeper-2 blob",
			blobName: "20250613.zookeeper-2_default_zookeeper-0ddb991b3eecffa8d5d7fb64e436b803a0b4e6f8a91aa7adce883a987e209fc9.gz",
			expected: true,
			reason:   "Zookeeper stateful set pod 2",
		},
		{
			name:     "different service should be excluded",
			blobName: "20250613.api-d64987b96-ctnd7_default_platform-c8c329006cf3420e14b2eb0bfa7d16f793cab8cae4347f26c84f5800aadfec70.gz",
			expected: false,
			reason:   "Different service (API), not zookeeper",
		},
		{
			name:     "invalid blob structure should be excluded",
			blobName: "invalid-blob-name.gz",
			expected: false,
			reason:   "Invalid blob name structure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := selector.Predicate(tt.blobName)
			assert.Equal(t, tt.expected, result, "Predicate result for '%s': %s", tt.blobName, tt.reason)
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
			expected: "kubernetes/20250607.apache2-igc-",
		},
		{
			name:     "different date",
			date:     "20251225",
			expected: "kubernetes/20251225.apache2-igc-",
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
		"kubernetes/20250607.apache2-igc-86888fc9d6-dzxxv_default_proxy-516459e2fb42d75a47ab803c19a317c180dd3b9d3f715ef722ed1539734a22d7.gz",
		"kubernetes/20250607.apache2-igc-nat-7c58b477db-42fg8_default_proxy-9dc8fce39dc3815ad7bf878d4c08d2556cf816f891acdb63e2fb7c5f5295acef.gz", // Should be excluded (NAT)
		"kubernetes/20250607.api-d64987b96-ctnd7_default_platform-c8c329006cf3420e14b2eb0bfa7d16f793cab8cae4347f26c84f5800aadfec70.gz",           // Wrong service
		"kubernetes/20250608.apache2-igc-86888fc9d6-dzxxv_default_proxy-516459e2fb42d75a47ab803c19a317c180dd3b9d3f715ef722ed1539734a22d7.gz",     // Wrong date
	}

	filtered := selector.FilterBlobsForDate(blobs, "20250607")

	expected := []string{
		"kubernetes/20250607.apache2-igc-86888fc9d6-dzxxv_default_proxy-516459e2fb42d75a47ab803c19a317c180dd3b9d3f715ef722ed1539734a22d7.gz",
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
