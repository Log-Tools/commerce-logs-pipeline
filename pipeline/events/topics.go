package events

// Kafka Topics
// These constants define the Kafka topics where events are published
const (
	// TopicBlobs contains blob lifecycle events (observed, listed, closed)
	TopicBlobs = "Ingestion.Blobs"

	// TopicRawProxyLogs contains proxy log lines extracted from blobs
	TopicRawProxyLogs = "Raw.ProxyLogs"

	// TopicRawApplicationLogs contains application log lines extracted from blobs
	TopicRawApplicationLogs = "Raw.ApplicationLogs"

	// TopicExtractedApplication contains structured application log events
	TopicExtractedApplication = "Extracted.Application"

	// TopicExtractionErrors contains extraction error events
	TopicExtractionErrors = "Extraction.Errors"

	// TopicState contains pipeline state and completion events
	TopicState = "Ingestion.State"

	// TopicBlobState contains compacted blob state (open/closed status and metadata)
	// This topic should be configured with log.cleanup.policy=compact
	TopicBlobState = "Ingestion.BlobState"
)
