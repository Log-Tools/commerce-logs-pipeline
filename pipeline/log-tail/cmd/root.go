package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"log-tail/internal/config"
	"log-tail/internal/consumer"

	"github.com/spf13/cobra"
)

var (
	configPath    string
	subscription  string
	environment   string
	service       string
	topics        []string
	outputFormat  string
	showRaw       bool
	consumerGroup string
	kafkaBrokers  string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "log-tail",
	Short: "Tail logs from Kafka topics with filtering capabilities",
	Long: `A CLI utility to tail logs from commerce pipeline Kafka topics.

Supports filtering by subscription, environment, and service selector.
Can consume from both raw and extracted log topics.

Examples:
  # Tail all logs
  log-tail

  # Filter by subscription and environment
  log-tail --subscription cp2 --environment P1

  # Filter by service
  log-tail --service api

  # Specify specific topics
  log-tail --topics Raw.ApplicationLogs,Extracted.Application

  # Output as JSON
  log-tail --format json

  # Show raw message metadata
  log-tail --raw`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runLogTail()
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&configPath, "config", "c", "", "config file path")
	rootCmd.PersistentFlags().StringVarP(&subscription, "subscription", "s", "", "subscription filter (e.g. cp2)")
	rootCmd.PersistentFlags().StringVarP(&environment, "environment", "e", "", "environment filter (e.g. P1, D1)")
	rootCmd.PersistentFlags().StringVar(&service, "service", "", "service filter (e.g. api, backoffice, backgroundprocessing)")
	rootCmd.PersistentFlags().StringSliceVarP(&topics, "topics", "t", nil, "specific topics to consume from (default: all log topics)")
	rootCmd.PersistentFlags().StringVarP(&outputFormat, "format", "f", "text", "output format: text, json")
	rootCmd.PersistentFlags().BoolVar(&showRaw, "raw", false, "show raw message metadata")
	rootCmd.PersistentFlags().StringVarP(&consumerGroup, "consumer-group", "g", "log-tail-cli", "Kafka consumer group ID")
	rootCmd.PersistentFlags().StringVar(&kafkaBrokers, "brokers", "", "Kafka brokers (overrides config)")
}

func runLogTail() error {
	// Load configuration
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Validate subscription and environment if provided
	if subscription != "" {
		if err := cfg.ValidateSubscriptionEnvironment(subscription, environment); err != nil {
			return fmt.Errorf("validation error: %w", err)
		}
	}

	// Determine Kafka brokers
	brokers := kafkaBrokers
	if brokers == "" {
		brokers = cfg.Kafka.Brokers
	}

	// Determine topics to consume from
	topicsToConsume := topics
	if len(topicsToConsume) == 0 {
		topicsToConsume = getDefaultTopics()
	}

	// Validate output format
	if outputFormat != "text" && outputFormat != "json" {
		return fmt.Errorf("invalid output format: %s (must be 'text' or 'json')", outputFormat)
	}

	// Print configuration summary
	fmt.Printf("🔧 Configuration Summary:\n")
	fmt.Printf("  Kafka Brokers: %s\n", brokers)
	fmt.Printf("  Consumer Group: %s\n", consumerGroup)
	fmt.Printf("  Topics: %s\n", strings.Join(topicsToConsume, ", "))
	if subscription != "" {
		fmt.Printf("  Subscription Filter: %s\n", subscription)
	}
	if environment != "" {
		fmt.Printf("  Environment Filter: %s\n", environment)
	}
	if service != "" {
		fmt.Printf("  Service Filter: %s\n", service)
	}
	fmt.Printf("  Output Format: %s\n", outputFormat)
	fmt.Printf("  Show Raw: %t\n", showRaw)
	fmt.Println()

	// Create log tailer
	opts := consumer.FilterOptions{
		Subscription: subscription,
		Environment:  environment,
		Service:      service,
		ShowRaw:      showRaw,
		OutputFormat: outputFormat,
	}

	tailer, err := consumer.NewLogTailer(brokers, consumerGroup, topicsToConsume, opts)
	if err != nil {
		return fmt.Errorf("failed to create log tailer: %w", err)
	}
	defer tailer.Close()

	// Setup signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-signalChan
		fmt.Println("\n🛑 Received shutdown signal...")
		cancel()
	}()

	// Start tailing logs
	return tailer.Start(ctx)
}

// getDefaultTopics returns the default set of topics to consume from
func getDefaultTopics() []string {
	return []string{
		"Raw.ApplicationLogs",
		"Raw.ProxyLogs",
		"Extracted.Application",
		"Extracted.Proxy",
		"Extraction.Errors",
	}
}
