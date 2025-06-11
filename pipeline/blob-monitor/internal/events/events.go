package events

// Re-export events from the shared module for backward compatibility
// This allows existing code to continue working while we transition
import (
	sharedEvents "github.com/Log-Tools/commerce-logs-pipeline/pipeline/events"
)

// Alias event types from shared module
type BlobObservedEvent = sharedEvents.BlobObservedEvent
type BlobsListedEvent = sharedEvents.BlobsListedEvent
type BlobClosedEvent = sharedEvents.BlobClosedEvent

// Topic constants
const (
	TopicBlobs = sharedEvents.TopicBlobs
)
