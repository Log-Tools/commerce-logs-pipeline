package service

import (
	"pipeline/events"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Consumer defines the interface for Kafka consumer operations
type Consumer interface {
	Subscribe(topics []string, rebalanceCb kafka.RebalanceCb) error
	ReadMessage(timeout int) (*kafka.Message, error)
	StoreMessage(message *kafka.Message) error
	CommitMessage(message *kafka.Message) error
	Close() error
}

// Producer defines the interface for Kafka producer operations
type Producer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Events() chan kafka.Event
	Flush(timeoutMs int) int
	Close()
}

// Extractor defines the interface for log extraction operations
type Extractor interface {
	ExtractLog(rawLine string, source events.LogSource) (interface{}, error)
	ExtractProxyLog(rawLine string, source events.LogSource) (interface{}, error)
	ValidateExtractedLog(log interface{}) error
}

// MetricsCollector defines the interface for collecting processing metrics
type MetricsCollector interface {
	IncrementMessagesProcessed()
	IncrementMessagesExtracted()
	IncrementExtractionErrors()
	IncrementValidationErrors()
	RecordProcessingLatency(durationMs int64)
}

// KafkaClientFactory defines the interface for creating Kafka clients
type KafkaClientFactory interface {
	CreateConsumer(brokers, groupID string, config map[string]interface{}) (Consumer, error)
	CreateProducer(brokers string, config map[string]interface{}) (Producer, error)
}

// ExtractorFactory defines the interface for creating extractors
type ExtractorFactory interface {
	CreateExtractor() Extractor
}

// MetricsFactory defines the interface for creating metrics collectors
type MetricsFactory interface {
	CreateMetricsCollector() MetricsCollector
}
