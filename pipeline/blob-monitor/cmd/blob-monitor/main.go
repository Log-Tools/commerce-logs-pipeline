package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Log-Tools/commerce-logs-pipeline/pipeline/blob-monitor/internal/service"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <config-file>", os.Args[0])
	}

	configPath := os.Args[1]

	blobService, err := service.NewBlobMonitorServiceFromFile(configPath)
	if err != nil {
		log.Fatalf("❌ Failed to create blob monitor service: %v", err)
	}

	ctx := context.Background()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("🛑 Received signal %v, initiating graceful shutdown...", sig)
		blobService.Stop()
	}()

	if err := blobService.Start(ctx); err != nil {
		log.Fatalf("❌ Service failed: %v", err)
	}
}
