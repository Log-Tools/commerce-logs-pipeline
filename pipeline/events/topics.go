package events

// Kafka Topics
// These constants define the Kafka topics where events are published
const (
	// TopicBlobs contains blob lifecycle events (observed, listed, closed)
	TopicBlobs = "Ingestion.Blobs"

	// TopicRawLogs contains the actual log lines extracted from blobs
	TopicRawLogs = "Ingestion.RawLogs"

	// TopicState contains pipeline state and completion events
	TopicState = "Ingestion.State"

	// TopicBlobState contains compacted blob state (open/closed status and metadata)
	// This topic should be configured with log.cleanup.policy=compact
	TopicBlobState = "Ingestion.BlobState"
)
