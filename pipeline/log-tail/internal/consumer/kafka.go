package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// LogTailer represents a Kafka consumer for tailing logs
type LogTailer struct {
	consumer     *kafka.Consumer
	topics       []string
	subscription string
	environment  string
	service      string
	outputFormat string
	showRaw      bool
}

// LogMessage represents a log message with metadata
type LogMessage struct {
	Topic     string            `json:"topic"`
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key,omitempty"`
	Value     string            `json:"value"`
	Timestamp time.Time         `json:"timestamp"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// FilterOptions represents filtering options for log consumption
type FilterOptions struct {
	Subscription string
	Environment  string
	Service      string
	ShowRaw      bool
	OutputFormat string // json, text
}

// NewLogTailer creates a new Kafka log consumer
func NewLogTailer(brokers string, consumerGroup string, topics []string, opts FilterOptions) (*LogTailer, error) {
	configMap := kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           consumerGroup,
		"auto.offset.reset":  "latest",
		"enable.auto.commit": true,
	}

	consumer, err := kafka.NewConsumer(&configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	return &LogTailer{
		consumer:     consumer,
		topics:       topics,
		subscription: opts.Subscription,
		environment:  opts.Environment,
		service:      opts.Service,
		outputFormat: opts.OutputFormat,
		showRaw:      opts.ShowRaw,
	}, nil
}

// Start begins consuming messages from Kafka topics
func (lt *LogTailer) Start(ctx context.Context) error {
	if err := lt.consumer.SubscribeTopics(lt.topics, nil); err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	fmt.Printf("🚀 Starting log tail for topics: %s\n", strings.Join(lt.topics, ", "))
	if lt.subscription != "" {
		fmt.Printf("📋 Subscription filter: %s\n", lt.subscription)
	}
	if lt.environment != "" {
		fmt.Printf("🌍 Environment filter: %s\n", lt.environment)
	}
	if lt.service != "" {
		fmt.Printf("⚙️ Service filter: %s\n", lt.service)
	}
	fmt.Printf("📄 Output format: %s\n", lt.outputFormat)
	fmt.Println("---")

	for {
		select {
		case <-ctx.Done():
			fmt.Println("\n🛑 Shutting down log tailer...")
			return nil
		default:
			msg, err := lt.consumer.ReadMessage(1000 * time.Millisecond)
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("❌ Consumer error: %v\n", err)
				continue
			}

			if lt.shouldProcessMessage(msg) {
				lt.displayMessage(msg)
			}
		}
	}
}

// shouldProcessMessage determines if a message matches the filters
func (lt *LogTailer) shouldProcessMessage(msg *kafka.Message) bool {
	key := string(msg.Key)

	// Parse key format: subscription:environment:eventType:blobName
	if key != "" {
		parts := strings.Split(key, ":")
		if len(parts) >= 2 {
			msgSubscription := parts[0]
			msgEnvironment := parts[1]

			// Filter by subscription
			if lt.subscription != "" && msgSubscription != lt.subscription {
				return false
			}

			// Filter by environment
			if lt.environment != "" && msgEnvironment != lt.environment {
				return false
			}
		}
	}

	// Filter by service (check in message content)
	if lt.service != "" {
		value := string(msg.Value)
		if !lt.containsService(value, lt.service) {
			return false
		}
	}

	return true
}

// containsService checks if the message contains the specified service
func (lt *LogTailer) containsService(messageValue, service string) bool {
	// Try to parse as JSON and look for service indicators
	var jsonMsg map[string]interface{}
	if err := json.Unmarshal([]byte(messageValue), &jsonMsg); err == nil {
		// Check Kubernetes pod name for service indicators
		if k8s, ok := jsonMsg["kubernetes"].(map[string]interface{}); ok {
			if podName, ok := k8s["pod_name"].(string); ok {
				if strings.Contains(strings.ToLower(podName), strings.ToLower(service)) {
					return true
				}
			}
		}

		// Check source field in extracted logs
		if source, ok := jsonMsg["source"].(map[string]interface{}); ok {
			if svc, ok := source["service"].(string); ok {
				if strings.EqualFold(svc, service) {
					return true
				}
			}
		}
	}

	// Fallback: simple string contains check
	return strings.Contains(strings.ToLower(messageValue), strings.ToLower(service))
}

// displayMessage formats and displays a Kafka message
func (lt *LogTailer) displayMessage(msg *kafka.Message) {
	logMsg := LogMessage{
		Topic:     *msg.TopicPartition.Topic,
		Partition: msg.TopicPartition.Partition,
		Offset:    int64(msg.TopicPartition.Offset),
		Key:       string(msg.Key),
		Value:     string(msg.Value),
		Timestamp: msg.Timestamp,
		Headers:   make(map[string]string),
	}

	// Extract headers
	for _, header := range msg.Headers {
		logMsg.Headers[header.Key] = string(header.Value)
	}

	switch lt.outputFormat {
	case "json":
		lt.displayAsJSON(logMsg)
	case "text":
		lt.displayAsText(logMsg)
	default:
		lt.displayAsText(logMsg)
	}
}

// displayAsJSON displays the message in JSON format
func (lt *LogTailer) displayAsJSON(msg LogMessage) {
	if lt.showRaw {
		// Show complete message metadata
		jsonBytes, _ := json.MarshalIndent(msg, "", "  ")
		fmt.Println(string(jsonBytes))
	} else {
		// Pretty print just the log content if it's JSON
		var prettyValue interface{}
		if err := json.Unmarshal([]byte(msg.Value), &prettyValue); err == nil {
			jsonBytes, _ := json.MarshalIndent(prettyValue, "", "  ")
			fmt.Printf("🕐 %s | 📝 %s | %s\n",
				msg.Timestamp.Format("15:04:05"),
				msg.Topic,
				string(jsonBytes))
		} else {
			fmt.Printf("🕐 %s | 📝 %s | %s\n",
				msg.Timestamp.Format("15:04:05"),
				msg.Topic,
				msg.Value)
		}
	}
}

// displayAsText displays the message in human-readable text format
func (lt *LogTailer) displayAsText(msg LogMessage) {
	if lt.showRaw {
		fmt.Printf("🕐 %s | 📝 %s[%d]@%d | 🔑 %s\n%s\n---\n",
			msg.Timestamp.Format("15:04:05.000"),
			msg.Topic,
			msg.Partition,
			msg.Offset,
			msg.Key,
			msg.Value)
	} else {
		// Try to extract meaningful content from JSON logs
		var jsonMsg map[string]interface{}
		if err := json.Unmarshal([]byte(msg.Value), &jsonMsg); err == nil {
			lt.displayStructuredLog(msg.Timestamp, msg.Topic, jsonMsg)
		} else {
			fmt.Printf("🕐 %s | 📝 %s | %s\n",
				msg.Timestamp.Format("15:04:05"),
				msg.Topic,
				msg.Value)
		}
	}
}

// displayStructuredLog displays structured log content in a readable format
func (lt *LogTailer) displayStructuredLog(timestamp time.Time, topic string, jsonMsg map[string]interface{}) {
	timeStr := timestamp.Format("15:04:05")

	// Handle different log types
	switch {
	case isHTTPRequestLog(jsonMsg):
		displayHTTPRequestLog(timeStr, topic, jsonMsg)
	case isApplicationLog(jsonMsg):
		displayApplicationLog(timeStr, topic, jsonMsg)
	case isProxyLog(jsonMsg):
		displayProxyLog(timeStr, topic, jsonMsg)
	default:
		// Generic display
		fmt.Printf("🕐 %s | 📝 %s | %v\n", timeStr, topic, jsonMsg)
	}
}

// isHTTPRequestLog checks if the log is an HTTP request log
func isHTTPRequestLog(jsonMsg map[string]interface{}) bool {
	_, hasMethod := jsonMsg["method"]
	_, hasPath := jsonMsg["path"]
	_, hasStatusCode := jsonMsg["status_code"]
	return hasMethod && hasPath && hasStatusCode
}

// isApplicationLog checks if the log is an application log
func isApplicationLog(jsonMsg map[string]interface{}) bool {
	_, hasLevel := jsonMsg["level"]
	_, hasMsg := jsonMsg["msg"]
	_, hasLogger := jsonMsg["logger"]
	return hasLevel && (hasMsg || hasLogger)
}

// isProxyLog checks if the log is a proxy log
func isProxyLog(jsonMsg map[string]interface{}) bool {
	_, hasLocalServer := jsonMsg["local_server_name"]
	_, hasUserAgent := jsonMsg["user_agent"]
	return hasLocalServer || hasUserAgent
}

// displayHTTPRequestLog displays HTTP request logs in a readable format
func displayHTTPRequestLog(timeStr, topic string, jsonMsg map[string]interface{}) {
	method := getStringValue(jsonMsg, "method")
	path := getStringValue(jsonMsg, "path")
	statusCode := getIntValue(jsonMsg, "status_code")
	responseTime := getIntValue(jsonMsg, "response_time_ms")
	clientIP := getStringValue(jsonMsg, "client_ip")
	podName := getStringValue(jsonMsg, "pod_name")

	fmt.Printf("🕐 %s | 🌐 %s | %s %s → %d (%dms) | 📍 %s | 🔧 %s\n",
		timeStr, topic, method, path, statusCode, responseTime, clientIP, podName)
}

// displayApplicationLog displays application logs in a readable format
func displayApplicationLog(timeStr, topic string, jsonMsg map[string]interface{}) {
	level := getStringValue(jsonMsg, "level")
	message := getStringValue(jsonMsg, "msg")
	logger := getStringValue(jsonMsg, "logger")
	podName := getStringValue(jsonMsg, "pod_name")

	levelEmoji := getLevelEmoji(level)

	fmt.Printf("🕐 %s | 📝 %s | %s [%s] %s | 🏷️  %s | 🔧 %s\n",
		timeStr, topic, levelEmoji, level, message, logger, podName)
}

// displayProxyLog displays proxy logs in a readable format
func displayProxyLog(timeStr, topic string, jsonMsg map[string]interface{}) {
	method := getStringValue(jsonMsg, "method")
	path := getStringValue(jsonMsg, "path")
	statusCode := getIntValue(jsonMsg, "status_code")
	responseTime := getIntValue(jsonMsg, "response_time_ms")
	clientIP := getStringValue(jsonMsg, "client_ip")
	localServer := getStringValue(jsonMsg, "local_server_name")

	fmt.Printf("🕐 %s | 🔄 %s | %s %s → %d (%dms) | 📍 %s | 🖥️  %s\n",
		timeStr, topic, method, path, statusCode, responseTime, clientIP, localServer)
}

// Helper functions
func getStringValue(jsonMsg map[string]interface{}, key string) string {
	if val, ok := jsonMsg[key].(string); ok {
		return val
	}
	return ""
}

func getIntValue(jsonMsg map[string]interface{}, key string) int {
	if val, ok := jsonMsg[key].(float64); ok {
		return int(val)
	}
	return 0
}

func getLevelEmoji(level string) string {
	switch strings.ToUpper(level) {
	case "ERROR":
		return "❌"
	case "WARN", "WARNING":
		return "⚠️"
	case "INFO":
		return "ℹ️"
	case "DEBUG":
		return "🔍"
	case "TRACE":
		return "🔬"
	default:
		return "📄"
	}
}

// Close closes the Kafka consumer
func (lt *LogTailer) Close() error {
	return lt.consumer.Close()
}
