package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"pipeline/extraction/internal/service"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <config-file>\n", os.Args[0])
		os.Exit(1)
	}

	configPath := os.Args[1]

	// Create service from configuration file
	svc, err := service.NewServiceFromFile(configPath)
	if err != nil {
		log.Fatalf("Failed to create service: %v", err)
	}
	defer svc.Close()

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start service
	log.Println("Starting application logs extraction service...")
	errChan := make(chan error, 1)
	go func() {
		errChan <- svc.Run(ctx)
	}()

	// Wait for shutdown signal or error
	select {
	case sig := <-sigChan:
		log.Printf("Received signal: %v. Shutting down gracefully...", sig)
		cancel()
	case err := <-errChan:
		if err != nil {
			log.Printf("Service error: %v", err)
			os.Exit(1)
		}
	}

	log.Println("Application logs extraction service stopped")
}
