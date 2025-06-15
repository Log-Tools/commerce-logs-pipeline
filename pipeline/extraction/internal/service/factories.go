package service

import (
	"pipeline/extraction/internal/extractor"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// DefaultKafkaClientFactory provides production Kafka client implementations
type DefaultKafkaClientFactory struct{}

func (f *DefaultKafkaClientFactory) CreateConsumer(brokers, groupID string, config map[string]interface{}) (Consumer, error) {
	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	}

	// Add custom configuration
	for key, value := range config {
		kafkaConfig[key] = value
	}

	consumer, err := kafka.NewConsumer(&kafkaConfig)
	if err != nil {
		return nil, err
	}

	return &KafkaConsumerWrapper{consumer}, nil
}

func (f *DefaultKafkaClientFactory) CreateProducer(brokers string, config map[string]interface{}) (Producer, error) {
	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers": brokers,
	}

	// Add custom configuration
	for key, value := range config {
		kafkaConfig[key] = value
	}

	producer, err := kafka.NewProducer(&kafkaConfig)
	if err != nil {
		return nil, err
	}

	return &KafkaProducerWrapper{producer}, nil
}

// DefaultExtractorFactory provides production extractor implementations
type DefaultExtractorFactory struct{}

func (f *DefaultExtractorFactory) CreateExtractor() Extractor {
	return extractor.NewExtractor()
}

// DefaultMetricsFactory provides production metrics collector implementations
type DefaultMetricsFactory struct{}

func (f *DefaultMetricsFactory) CreateMetricsCollector() MetricsCollector {
	return &SimpleMetricsCollector{}
}

// KafkaConsumerWrapper wraps the confluent-kafka-go consumer to implement our Consumer interface
type KafkaConsumerWrapper struct {
	*kafka.Consumer
}

func (w *KafkaConsumerWrapper) Subscribe(topics []string, rebalanceCb kafka.RebalanceCb) error {
	return w.Consumer.Subscribe(topics[0], rebalanceCb) // Simplified for single topic
}

func (w *KafkaConsumerWrapper) ReadMessage(timeout int) (*kafka.Message, error) {
	return w.Consumer.ReadMessage(time.Duration(timeout) * time.Millisecond)
}

func (w *KafkaConsumerWrapper) StoreMessage(message *kafka.Message) error {
	_, err := w.Consumer.StoreMessage(message)
	return err
}

func (w *KafkaConsumerWrapper) CommitMessage(message *kafka.Message) error {
	_, err := w.Consumer.CommitMessage(message)
	return err
}

// KafkaProducerWrapper wraps the confluent-kafka-go producer to implement our Producer interface
type KafkaProducerWrapper struct {
	*kafka.Producer
}

func (w *KafkaProducerWrapper) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	return w.Producer.Produce(msg, deliveryChan)
}

func (w *KafkaProducerWrapper) Events() chan kafka.Event {
	return w.Producer.Events()
}

func (w *KafkaProducerWrapper) Flush(timeoutMs int) int {
	return w.Producer.Flush(timeoutMs)
}

func (w *KafkaProducerWrapper) Close() {
	w.Producer.Close()
}

// ExtractorWrapper is no longer needed since our extractor implements the interface directly

// SimpleMetricsCollector provides a basic metrics collector implementation
type SimpleMetricsCollector struct {
	messagesProcessed   int64
	messagesExtracted   int64
	extractionErrors    int64
	validationErrors    int64
	totalProcessingTime int64
}

func (m *SimpleMetricsCollector) IncrementMessagesProcessed() {
	m.messagesProcessed++
}

func (m *SimpleMetricsCollector) IncrementMessagesExtracted() {
	m.messagesExtracted++
}

func (m *SimpleMetricsCollector) IncrementExtractionErrors() {
	m.extractionErrors++
}

func (m *SimpleMetricsCollector) IncrementValidationErrors() {
	m.validationErrors++
}

func (m *SimpleMetricsCollector) RecordProcessingLatency(durationMs int64) {
	m.totalProcessingTime += durationMs
}
