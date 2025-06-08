package main

import (
	"fmt"
	"strings"
)

// BlobSelector combines Azure prefix with predicate function for blob filtering
type BlobSelector struct {
	Name        string                     // Selector identifier
	DisplayName string                     // Human-readable name
	Description string                     // Description of what this selector matches
	AzurePrefix string                     // Prefix for Azure list blob requests
	Predicate   func(blobName string) bool // Function to filter blobs
}

// GetBlobSelectors returns the map of all available blob selectors
// This is the single source of truth for selector definitions
func GetBlobSelectors() map[string]*BlobSelector {
	return map[string]*BlobSelector{
		"apache-proxy": {
			Name:        "apache-proxy",
			DisplayName: "Apache Proxy Service",
			Description: "Load balancer and proxy logs",
			AzurePrefix: "kubernetes/",
			Predicate: func(blobName string) bool {
				return strings.Contains(blobName, "apache2-igc") &&
					strings.Contains(blobName, "_proxy-") &&
					!strings.Contains(blobName, "_cache-cleaner-")
			},
		},

		"api": {
			Name:        "api",
			DisplayName: "Commerce API Service",
			Description: "Main API service logs",
			AzurePrefix: "kubernetes/",
			Predicate: func(blobName string) bool {
				return strings.Contains(blobName, "_api-") &&
					!strings.Contains(blobName, "_cache-cleaner-") &&
					!strings.Contains(blobName, "_log-forwarder-")
			},
		},

		"backoffice": {
			Name:        "backoffice",
			DisplayName: "Backoffice Service",
			Description: "Administrative interface logs",
			AzurePrefix: "kubernetes/",
			Predicate: func(blobName string) bool {
				return strings.Contains(blobName, "backoffice") &&
					!strings.Contains(blobName, "_cache-cleaner-")
			},
		},

		"background-processing": {
			Name:        "background-processing",
			DisplayName: "Background Processing Service",
			Description: "Asynchronous task processing logs",
			AzurePrefix: "kubernetes/",
			Predicate: func(blobName string) bool {
				return strings.Contains(blobName, "backgroundprocessing") &&
					!strings.Contains(blobName, "_cache-cleaner-") &&
					!strings.Contains(blobName, "_log-forwarder-")
			},
		},

		"jsapps": {
			Name:        "jsapps",
			DisplayName: "JavaScript Applications",
			Description: "Frontend application logs",
			AzurePrefix: "kubernetes/",
			Predicate: func(blobName string) bool {
				return strings.Contains(blobName, "jsapps") &&
					!strings.Contains(blobName, "_cache-cleaner-")
			},
		},

		"imageprocessing": {
			Name:        "imageprocessing",
			DisplayName: "Image Processing Service",
			Description: "Media and image processing logs",
			AzurePrefix: "kubernetes/",
			Predicate: func(blobName string) bool {
				return strings.Contains(blobName, "imageprocessing") &&
					!strings.Contains(blobName, "_cache-cleaner-")
			},
		},

		"zookeeper": {
			Name:        "zookeeper",
			DisplayName: "Zookeeper Service",
			Description: "Zookeeper coordination service logs",
			AzurePrefix: "kubernetes/",
			Predicate: func(blobName string) bool {
				return strings.Contains(blobName, "zookeeper")
			},
		},
	}
}

// ValidateSelector checks if a selector name exists in the available selectors
func ValidateSelector(selectorName string) error {
	selectors := GetBlobSelectors()
	if _, exists := selectors[selectorName]; !exists {
		return fmt.Errorf("unknown selector '%s'. Available selectors: %v",
			selectorName, getAvailableSelectorNames())
	}
	return nil
}

// GetSelector retrieves a selector by name
func GetSelector(selectorName string) (*BlobSelector, error) {
	selectors := GetBlobSelectors()
	selector, exists := selectors[selectorName]
	if !exists {
		return nil, fmt.Errorf("selector '%s' not found", selectorName)
	}
	return selector, nil
}

// getAvailableSelectorNames returns a list of all available selector names
func getAvailableSelectorNames() []string {
	selectors := GetBlobSelectors()
	names := make([]string, 0, len(selectors))
	for name := range selectors {
		names = append(names, name)
	}
	return names
}

// FilterBlobsForDate adds date filtering to selector predicate
func (s *BlobSelector) FilterBlobsForDate(blobs []string, date string) []string {
	datePrefix := s.AzurePrefix + date + "."
	var filtered []string

	for _, blob := range blobs {
		// First check if blob matches the date
		if strings.HasPrefix(blob, datePrefix) {
			// Then apply the selector's predicate
			if s.Predicate(blob) {
				filtered = append(filtered, blob)
			}
		}
	}

	return filtered
}

// GetDatePrefix returns the Azure prefix for a specific date
func (s *BlobSelector) GetDatePrefix(date string) string {
	return s.AzurePrefix + date + "."
}
