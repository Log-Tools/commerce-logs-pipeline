package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
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
		verbose    = flag.Bool("verbose", false, "Show detailed topic configurations")
		dryRun     = flag.Bool("dry-run", false, "Show what would be created without actually creating topics")
	)
	flag.Parse()

	// Read and parse configuration
	content, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("❌ Unable to read %s: %v", *configPath, err)
	}

	var tf TopicFile
	if err := yaml.Unmarshal(content, &tf); err != nil {
		log.Fatalf("❌ Invalid YAML in %s: %v", *configPath, err)
	}

	if len(tf.Topics) == 0 {
		log.Printf("⚠️  No topics defined in %s", *configPath)
		return
	}

	if *dryRun {
		fmt.Printf("🔍 Dry run mode - would create/verify %d topic(s):\n", len(tf.Topics))
		// Sort topics for consistent output
		var topicNames []string
		for name := range tf.Topics {
			topicNames = append(topicNames, name)
		}
		sort.Strings(topicNames)

		for _, name := range topicNames {
			spec := tf.Topics[name]
			fmt.Printf("   📋 %s (partitions: %d, cleanup: %s)\n", name, spec.Partitions, spec.CleanupPolicy)
			if *verbose {
				for k, v := range spec.Other {
					if k != "partitions" && k != "cleanup.policy" {
						fmt.Printf("      %s: %v\n", k, v)
					}
				}
			}
		}
		return
	}

	// Create admin client
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": *brokerList})
	if err != nil {
		log.Fatalf("❌ Failed to create admin client: %v", err)
	}
	defer admin.Close()

	// Test connectivity
	if *verbose {
		fmt.Printf("🔗 Connecting to Kafka brokers: %s\n", *brokerList)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Build topic specifications
	var specs []kafka.TopicSpecification
	var topicNames []string
	for name := range tf.Topics {
		topicNames = append(topicNames, name)
	}
	sort.Strings(topicNames)

	for _, name := range topicNames {
		t := tf.Topics[name]
		cfg := map[string]string{"cleanup.policy": t.CleanupPolicy}

		// Copy other configurations
		for k, v := range t.Other {
			// Skip known explicit fields
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

		if *verbose {
			fmt.Printf("📋 Configuring topic %s:\n", name)
			fmt.Printf("   Partitions: %d\n", t.Partitions)
			fmt.Printf("   Replication: 1\n")
			for k, v := range cfg {
				fmt.Printf("   %s: %s\n", k, v)
			}
		}
	}

	// Create topics
	results, err := admin.CreateTopics(ctx, specs, kafka.SetAdminOperationTimeout(30*time.Second))
	if err != nil {
		log.Fatalf("❌ CreateTopics request failed: %v", err)
	}

	// Process results
	var created, existing, failed int
	for _, res := range results {
		if res.Error.Code() == kafka.ErrTopicAlreadyExists {
			log.Printf("✓ %s already exists", res.Topic)
			existing++
		} else if res.Error.Code() == kafka.ErrNoError {
			log.Printf("✓ created %s", res.Topic)
			created++
		} else {
			log.Printf("✗ %s: %v", res.Topic, res.Error)
			failed++
		}
	}

	// Summary
	if created > 0 || existing > 0 {
		fmt.Printf("📊 Summary: %d created, %d existing, %d failed\n", created, existing, failed)
	}

	if failed > 0 {
		os.Exit(1)
	}
}
