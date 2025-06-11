package events

import (
	"fmt"
	"strings"
)

// BlobEventKey represents the components of a blob event key
type BlobEventKey struct {
	Subscription string
	Environment  string
	EventType    string
	BlobName     string
}

// GenerateBlobEventKey creates a standardized key for blob events
// Format: {subscription}:{environment}:{eventType}:{cleanBlobName}
// Using colons as delimiters to avoid conflicts with dashes in event types and blob names
func GenerateBlobEventKey(subscription, environment, eventType, blobName string) string {
	// Remove kubernetes/ prefix if present
	cleanBlobName := blobName
	if strings.HasPrefix(blobName, "kubernetes/") {
		cleanBlobName = strings.TrimPrefix(blobName, "kubernetes/")
	}

	return fmt.Sprintf("%s:%s:%s:%s", subscription, environment, eventType, cleanBlobName)
}

// ParseBlobEventKey parses a blob event key into its components
// Returns error if the key doesn't have the expected format
func ParseBlobEventKey(key string) (*BlobEventKey, error) {
	// Split into exactly 4 parts: subscription, environment, eventType, blobName
	parts := strings.SplitN(key, ":", 4)
	if len(parts) != 4 {
		return nil, fmt.Errorf("invalid blob event key format: expected exactly 4 parts separated by colons, got %d parts: %s", len(parts), key)
	}

	return &BlobEventKey{
		Subscription: parts[0],
		Environment:  parts[1],
		EventType:    parts[2],
		BlobName:     parts[3],
	}, nil
}

// String returns the key in the standard format
func (k *BlobEventKey) String() string {
	return GenerateBlobEventKey(k.Subscription, k.Environment, k.EventType, k.BlobName)
}

// IsEventType checks if the key matches a specific event type
func (k *BlobEventKey) IsEventType(eventType string) bool {
	return k.EventType == eventType
}

// IsLogLine checks if this is a log line event (starts with "line-")
func (k *BlobEventKey) IsLogLine() bool {
	return strings.HasPrefix(k.EventType, "line-")
}

// IsCompletion checks if this is a completion event (ends with "-completion")
func (k *BlobEventKey) IsCompletion() bool {
	return strings.HasSuffix(k.EventType, "-completion")
}

// IsObserved checks if this is a blob observed event
func (k *BlobEventKey) IsObserved() bool {
	return k.EventType == "observed"
}

// IsClosed checks if this is a blob closed event
func (k *BlobEventKey) IsClosed() bool {
	return k.EventType == "closed"
}
