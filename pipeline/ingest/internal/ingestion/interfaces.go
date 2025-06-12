package ingestion

import (
	"context"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Producer interface abstracts Kafka producer
type Producer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Events() chan kafka.Event
	Flush(timeoutMs int) int
	Close()
}

// Consumer interface abstracts Kafka consumer
type Consumer interface {
	Subscribe(topics []string, rebalanceCb kafka.RebalanceCb) error
	Poll(timeoutMs int) kafka.Event
	Commit() ([]kafka.TopicPartition, error)
	Close() error
}

// BlobClient interface abstracts Azure Blob Storage client
type BlobClient interface {
	GetProperties(ctx context.Context, options *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error)
	DownloadStream(ctx context.Context, options *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error)
}

// StorageClientFactory interface abstracts storage client creation
type StorageClientFactory interface {
	CreateBlobClient(subscription, environment, containerName, blobName string) (BlobClient, error)
}

// BlobProcessor interface defines blob processing operations
type BlobProcessor interface {
	ProcessBlob(ctx context.Context, blobInfo BlobProcessingInfo) (*BlobProcessingResult, error)
}

// BlobProcessingResult contains the results of blob processing
type BlobProcessingResult struct {
	ProcessedToOffset int64 // The byte offset up to which processing completed
	LinesProcessed    int   // Number of lines processed in this run
}

// BlobProcessingInfo contains information needed to process a blob
type BlobProcessingInfo struct {
	ContainerName string
	BlobName      string
	StartOffset   int64
	Subscription  string
	Environment   string
}

// BlobStateEvent represents a blob state event from Kafka
type BlobStateEvent struct {
	BlobName         string    `json:"blobName"`
	ContainerName    string    `json:"containerName"`
	Subscription     string    `json:"subscription"`
	Environment      string    `json:"environment"`
	Selector         string    `json:"selector"`
	State            string    `json:"state"` // "open" or "closed"
	SizeInBytes      int64     `json:"sizeInBytes"`
	LastModified     time.Time `json:"lastModified"`
	LastIngestedByte int64     `json:"lastIngestedByte"`
}
