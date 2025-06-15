package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"pipeline/extraction/internal/config"
	"pipeline/extraction/internal/service"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <config-file>\n", os.Args[0])
		os.Exit(1)
	}

	configPath := os.Args[1]

	// Load configuration first
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Create application logs service from configuration
	appSvc, err := service.NewServiceWithConfig(cfg)
	if err != nil {
		log.Fatalf("Failed to create application logs service: %v", err)
	}
	defer appSvc.Close()

	// Create proxy logs service from configuration
	proxySvc, err := service.NewProxyServiceWithConfig(cfg)
	if err != nil {
		log.Fatalf("Failed to create proxy logs service: %v", err)
	}
	defer proxySvc.Close()

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Use WaitGroup to coordinate both services
	var wg sync.WaitGroup
	errChan := make(chan error, 2)

	// Start application logs extraction service
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("Starting application logs extraction service...")
		if err := appSvc.Run(ctx); err != nil {
			errChan <- fmt.Errorf("application service error: %w", err)
		}
	}()

	// Start proxy logs extraction service
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("Starting proxy logs extraction service...")
		if err := proxySvc.Run(ctx); err != nil {
			errChan <- fmt.Errorf("proxy service error: %w", err)
		}
	}()

	// Wait for shutdown signal or error
	select {
	case sig := <-sigChan:
		log.Printf("Received signal: %v. Shutting down gracefully...", sig)
		cancel()
	case err := <-errChan:
		log.Printf("Service error: %v", err)
		cancel()
	}

	// Wait for both services to stop
	wg.Wait()
	log.Println("Both application and proxy extraction services stopped")
}
