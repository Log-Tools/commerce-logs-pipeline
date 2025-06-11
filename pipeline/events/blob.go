package events

import "time"

// BlobObservedEvent represents a single blob discovery event
// Published when blob-monitor or ingest pipeline detects a blob
type BlobObservedEvent struct {
	Subscription     string    `json:"subscription"`
	Environment      string    `json:"environment"`
	BlobName         string    `json:"blobName"`
	ObservationDate  time.Time `json:"observationDate"`
	SizeInBytes      int64     `json:"sizeInBytes"`
	LastModifiedDate time.Time `json:"lastModifiedDate"`
	CreationDate     time.Time `json:"creationDate,omitempty"`
	ServiceSelector  string    `json:"serviceSelector,omitempty"` // Which selector matched this blob (from blob-monitor)
}

// BlobsListedEvent indicates completion of blob listing for a specific date/service/environment
// Published by blob-monitor when finishing a scan operation
type BlobsListedEvent struct {
	Subscription     string    `json:"subscription"`
	Environment      string    `json:"environment"`
	ServiceSelector  string    `json:"serviceSelector"`
	Date             string    `json:"date"`             // YYYYMMDD format
	ListingStartTime time.Time `json:"listingStartTime"` // When the listing request was initiated
	ListingEndTime   time.Time `json:"listingEndTime"`   // When the listing request completed
	BlobCount        int       `json:"blobCount"`        // Number of blobs found for this selector
	TotalBytes       int64     `json:"totalBytes"`       // Total size of all blobs found
}

// BlobClosedEvent indicates a blob is considered closed (no new data expected)
// Published by blob-monitor when a blob exceeds the configured inactivity timeout
type BlobClosedEvent struct {
	Subscription     string    `json:"subscription"`
	Environment      string    `json:"environment"`
	BlobName         string    `json:"blobName"`
	ServiceSelector  string    `json:"serviceSelector"`
	LastModifiedDate time.Time `json:"lastModifiedDate"` // When the blob was last modified
	ClosedDate       time.Time `json:"closedDate"`       // When we determined it was closed
	SizeInBytes      int64     `json:"sizeInBytes"`      // Final size when closed
	TimeoutMinutes   int       `json:"timeoutMinutes"`   // Configured timeout that triggered closure
}

// BlobCompletionEvent represents completion of blob segment processing
// Published by ingest pipeline when a blob segment has been fully processed
type BlobCompletionEvent struct {
	BlobName             string    `json:"blobName"`
	ProcessedFromOffset  int64     `json:"processedFromOffset"`
	ProcessedToEndOffset int64     `json:"processedToEndOffset"`
	LinesSent            int       `json:"linesSent"`
	Subscription         string    `json:"subscription"`
	Environment          string    `json:"environment"`
	ProcessedDate        time.Time `json:"processedDate"`
}

// BlobStateEvent represents the current state of a blob in the compacted topic
// This is the canonical representation of blob state derived from all other events
type BlobStateEvent struct {
	// Key components (used for topic key)
	Subscription string `json:"subscription"`
	Environment  string `json:"environment"`
	BlobName     string `json:"blobName"`

	// State information
	Status      string    `json:"status"`      // "open" or "closed"
	LastUpdated time.Time `json:"lastUpdated"` // When this state was last updated

	// Blob metadata
	SizeInBytes      int64     `json:"sizeInBytes"`      // Current known size
	LastModifiedDate time.Time `json:"lastModifiedDate"` // When blob was last modified in storage
	ServiceSelector  string    `json:"serviceSelector"`  // Which service selector matches this blob

	// Processing state
	LastIngestedOffset int64      `json:"lastIngestedOffset"`          // Highest byte offset that has been ingested
	TotalLinesIngested int        `json:"totalLinesIngested"`          // Total lines processed from this blob
	CreationDate       time.Time  `json:"creationDate,omitempty"`      // When blob was created in storage (if known)
	LastProcessedDate  *time.Time `json:"lastProcessedDate,omitempty"` // When last ingestion completed

	// Discovery information
	FirstObservedDate time.Time `json:"firstObservedDate"` // When blob was first discovered
	LastObservedDate  time.Time `json:"lastObservedDate"`  // When blob was last seen
}
