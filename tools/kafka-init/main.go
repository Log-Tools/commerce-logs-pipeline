package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// TopicSpec represents configuration for a single Kafka topic read from YAML
type TopicSpec struct {
	Partitions    int                    `yaml:"partitions"`
	CleanupPolicy string                 `yaml:"cleanup.policy"`
	Other         map[string]interface{} `yaml:",inline"`
}

type TopicFile struct {
	Topics map[string]TopicSpec `yaml:"topics"`
}

func main() {
	var (
		brokerList = flag.String("brokers", "localhost:9092", "Comma-separated list of bootstrap brokers")
		configPath = flag.String("config", "configs/kafka_topics.yaml", "Path to kafka_topics.yaml")
	)
	flag.Parse()

	content, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("unable to read %s: %v", *configPath, err)
	}

	var tf TopicFile
	if err := yaml.Unmarshal(content, &tf); err != nil {
		log.Fatalf("invalid yaml in %s: %v", *configPath, err)
	}

	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": *brokerList})
	if err != nil {
		log.Fatalf("failed to create admin client: %v", err)
	}
	defer admin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var specs []kafka.TopicSpecification
	for name, t := range tf.Topics {
		cfg := map[string]string{"cleanup.policy": t.CleanupPolicy}
		// copy other configs
		for k, v := range t.Other {
			// skip known explicit fields
			if k == "partitions" || k == "cleanup.policy" {
				continue
			}
			cfg[k] = fmt.Sprint(v)
		}
		specs = append(specs, kafka.TopicSpecification{
			Topic:             name,
			NumPartitions:     t.Partitions,
			ReplicationFactor: 1,
			Config:            cfg,
		})
	}

	results, err := admin.CreateTopics(ctx, specs, kafka.SetAdminOperationTimeout(30*time.Second))
	if err != nil {
		log.Fatalf("CreateTopics request failed: %v", err)
	}

	for _, res := range results {
		if res.Error.Code() == kafka.ErrTopicAlreadyExists {
			log.Printf("✓ %s already exists", res.Topic)
		} else if res.Error.Code() == kafka.ErrNoError {
			log.Printf("✓ created %s", res.Topic)
		} else {
			log.Printf("✗ %s: %v", res.Topic, res.Error)
		}
	}
}
