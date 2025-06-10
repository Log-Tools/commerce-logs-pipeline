package events

import "time"

// BlobObservedEvent represents a single blob discovery event
type BlobObservedEvent struct {
	Subscription     string    `json:"subscription"`
	Environment      string    `json:"environment"`
	BlobName         string    `json:"blobName"`
	ObservationDate  time.Time `json:"observationDate"`
	SizeInBytes      int64     `json:"sizeInBytes"`
	LastModifiedDate time.Time `json:"lastModifiedDate"`
	ServiceSelector  string    `json:"serviceSelector"` // Which selector matched this blob
}

// BlobsListedEvent indicates completion of blob listing for a specific date/service/environment
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
