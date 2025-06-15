package selectors

import (
	"fmt"
	"strings"
)

// KubernetesBlobName represents the parsed structure of a Kubernetes log blob name
// Format: {date}.{pod-name}_{namespace}_{container-name}-{container-id}.gz
type KubernetesBlobName struct {
	Date        string // YYYYMMDD
	PodName     string // e.g., "apache2-igc-86888fc9d6-dzxxv"
	Namespace   string // e.g., "default"
	Container   string // e.g., "proxy"
	ContainerID string // e.g., "516459e2fb42d75a47ab803c19a317c180dd3b9d3f715ef722ed1539734a22d7"
	Valid       bool   // Whether the blob name follows the expected structure
}

// ParseKubernetesBlobName parses a blob name into its structural components
func ParseKubernetesBlobName(blobName string) KubernetesBlobName {
	result := KubernetesBlobName{Valid: false}

	// Must end with .gz
	if !strings.HasSuffix(blobName, ".gz") {
		return result
	}

	// Remove .gz extension
	nameWithoutExt := strings.TrimSuffix(blobName, ".gz")

	// Find first dot (separates date from pod name)
	dotIndex := strings.Index(nameWithoutExt, ".")
	if dotIndex == -1 {
		return result
	}

	result.Date = nameWithoutExt[:dotIndex]
	remainder := nameWithoutExt[dotIndex+1:]

	// Find first underscore (separates pod name from namespace_container)
	underscoreIndex := strings.Index(remainder, "_")
	if underscoreIndex == -1 {
		return result
	}

	result.PodName = remainder[:underscoreIndex]
	namespaceContainer := remainder[underscoreIndex+1:]

	// Find last dash that separates container name from container ID
	// We need the last dash because container names can have dashes (e.g., "hybris-autoscaler")
	lastDashIndex := strings.LastIndex(namespaceContainer, "-")
	if lastDashIndex == -1 {
		return result
	}

	// Split namespace_container from container-id
	namespaceContainerPart := namespaceContainer[:lastDashIndex]
	result.ContainerID = namespaceContainer[lastDashIndex+1:]

	// Find last underscore in namespace_container part
	lastUnderscoreIndex := strings.LastIndex(namespaceContainerPart, "_")
	if lastUnderscoreIndex == -1 {
		return result
	}

	result.Namespace = namespaceContainerPart[:lastUnderscoreIndex]
	result.Container = namespaceContainerPart[lastUnderscoreIndex+1:]
	result.Valid = true

	return result
}

// BlobSelector combines Azure prefix with predicate function for blob filtering
type BlobSelector struct {
	Name          string                     // Selector identifier
	DisplayName   string                     // Human-readable name
	Description   string                     // Description of what this selector matches
	AzurePrefix   string                     // Base prefix for Azure list blob requests
	ServicePrefix string                     // Service-specific prefix (e.g., ".api-", ".apache2-igc")
	Predicate     func(blobName string) bool // Function to filter blobs
}

// Provides centralized registry of all blob filtering rules for consistent service selection
func GetBlobSelectors() map[string]*BlobSelector {
	return map[string]*BlobSelector{
		"apache-proxy": {
			Name:          "apache-proxy",
			DisplayName:   "Apache Proxy Service",
			Description:   "HTTP request/response logs from Apache proxy containers",
			AzurePrefix:   "kubernetes/",
			ServicePrefix: ".apache2-igc-",
			Predicate: func(blobName string) bool {
				parsed := ParseKubernetesBlobName(blobName)
				return parsed.Valid &&
					parsed.Namespace == "default" &&
					parsed.Container == "proxy" &&
					strings.HasPrefix(parsed.PodName, "apache2-igc-") &&
					!strings.HasPrefix(parsed.PodName, "apache2-igc-nat-")
			},
		},

		"api": {
			Name:          "api",
			DisplayName:   "Commerce API Service",
			Description:   "Main API service logs",
			AzurePrefix:   "kubernetes/",
			ServicePrefix: ".api-",
			Predicate: func(blobName string) bool {
				parsed := ParseKubernetesBlobName(blobName)
				return parsed.Valid &&
					strings.HasPrefix(parsed.PodName, "api-") &&
					parsed.Container == "platform"
			},
		},

		"backoffice": {
			Name:          "backoffice",
			DisplayName:   "Backoffice Service",
			Description:   "Administrative interface logs",
			AzurePrefix:   "kubernetes/",
			ServicePrefix: ".backoffice",
			Predicate: func(blobName string) bool {
				parsed := ParseKubernetesBlobName(blobName)
				return parsed.Valid &&
					strings.HasPrefix(parsed.PodName, "backoffice") &&
					parsed.Container == "platform"
			},
		},

		"background-processing": {
			Name:          "background-processing",
			DisplayName:   "Background Processing Service",
			Description:   "Asynchronous task processing logs",
			AzurePrefix:   "kubernetes/",
			ServicePrefix: ".backgroundprocessing",
			Predicate: func(blobName string) bool {
				parsed := ParseKubernetesBlobName(blobName)
				return parsed.Valid &&
					strings.HasPrefix(parsed.PodName, "backgroundprocessing") &&
					parsed.Container == "platform"
			},
		},

		"jsapps": {
			Name:          "jsapps",
			DisplayName:   "JavaScript Applications",
			Description:   "Frontend application logs",
			AzurePrefix:   "kubernetes/",
			ServicePrefix: ".jsapps",
			Predicate: func(blobName string) bool {
				parsed := ParseKubernetesBlobName(blobName)
				return parsed.Valid &&
					strings.HasPrefix(parsed.PodName, "jsapps") &&
					parsed.Container == "jsapps"
			},
		},

		"imageprocessing": {
			Name:          "imageprocessing",
			DisplayName:   "Image Processing Service",
			Description:   "Media and image processing logs",
			AzurePrefix:   "kubernetes/",
			ServicePrefix: ".imageprocessing",
			Predicate: func(blobName string) bool {
				parsed := ParseKubernetesBlobName(blobName)
				return parsed.Valid &&
					strings.HasPrefix(parsed.PodName, "imageprocessing") &&
					parsed.Container == "imageprocessing"
			},
		},

		"zookeeper": {
			Name:          "zookeeper",
			DisplayName:   "Zookeeper Service",
			Description:   "Zookeeper coordination service logs",
			AzurePrefix:   "kubernetes/",
			ServicePrefix: ".zookeeper",
			Predicate: func(blobName string) bool {
				parsed := ParseKubernetesBlobName(blobName)
				return parsed.Valid &&
					strings.HasPrefix(parsed.PodName, "zookeeper") &&
					parsed.Container == "zookeeper"
			},
		},
	}
}

// Prevents runtime errors by rejecting unknown selector names during configuration parsing
func ValidateSelector(selectorName string) error {
	selectors := GetBlobSelectors()
	if _, exists := selectors[selectorName]; !exists {
		return fmt.Errorf("unknown selector '%s'. Available selectors: %v",
			selectorName, getAvailableSelectorNames())
	}
	return nil
}

// Retrieves configured selector for runtime blob filtering operations
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

// Applies both date-based filtering and selector predicates to optimize blob processing
func (s *BlobSelector) FilterBlobsForDate(blobs []string, date string) []string {
	datePrefix := s.AzurePrefix + date + s.ServicePrefix
	var filtered []string

	for _, blob := range blobs {
		// First check if blob matches the date and service
		if strings.HasPrefix(blob, datePrefix) {
			// Then apply the selector's predicate
			if s.Predicate(blob) {
				filtered = append(filtered, blob)
			}
		}
	}

	return filtered
}

// Constructs Azure Storage prefix to minimize network overhead during blob listing
func (s *BlobSelector) GetDatePrefix(date string) string {
	return s.AzurePrefix + date + s.ServicePrefix
}
