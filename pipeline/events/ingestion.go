package events

// IngestionCompletedEvent defines the structure for ingestion completion events
// Published by the ingest pipeline when a blob segment has been fully processed
// The ProcessedToEndOffset indicates the byte offset in the blob up to which
// processing has been successfully completed for this run
type IngestionCompletedEvent struct {
	BlobName             string `json:"blobName"`
	ProcessedFromOffset  int64  `json:"processedFromOffset"`
	ProcessedToEndOffset int64  `json:"processedToEndOffset"`
	LinesSent            int    `json:"linesSent"`
	Subscription         string `json:"subscription"`
	Environment          string `json:"environment"`
}
