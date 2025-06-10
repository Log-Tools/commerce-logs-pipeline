# Commerce Logs Pipeline - Docker Operations & Go Builds
.PHONY: help dev-up dev-down build test clean build-go clean-go test-go install-go

# Go modules
MODULES := pipeline/blob-monitor pipeline/ingest

# Default target
help:
	@echo "Commerce Logs Pipeline - Commands"
	@echo "================================="
	@echo "Go Development:"
	@echo "  make build-go        Build all Go modules to bin/ directories"
	@echo "  make clean-go        Clean all Go module binaries"
	@echo "  make test-go         Run tests for all Go modules"
	@echo "  make install-go      Install all Go modules to GOPATH/bin"
	@echo ""
	@echo "Module-specific (development):"
	@echo "  make run-blob-monitor Run blob monitor with example config"
	@echo "  make test-blob-monitor Run blob monitor tests only"
	@echo "  make run-ingest      Run ingestion pipeline (requires env vars)"
	@echo ""
	@echo "Development (recommended):"
	@echo "  make dev-up          Start Kafka & Kafdrop for local development"
	@echo "  make dev-down        Stop development services"
	@echo ""
	@echo "Production testing:"
	@echo "  make build-ingest    Build ingestion pipeline image"
	@echo "  make test-ingest     Run ingestion pipeline in container"
	@echo ""
	@echo "Data management:"
	@echo "  make wipe-topics     Delete all Kafka topics (soft reset)"
	@echo "  make wipe-data       Remove all Kafka data (hard reset)"
	@echo "  make list-topics     Show current Kafka topics"
	@echo ""
	@echo "Utilities:"
	@echo "  make logs           Show logs from all services"
	@echo "  make clean          Remove all containers and images"
	@echo "  make test-kafka     Test Kafka connectivity"

# Go build targets
build-go:
	@echo "🔨 Building all Go modules..."
	@for module in $(MODULES); do \
		echo "Building $$module..."; \
		$(MAKE) -C $$module build; \
	done
	@echo "✅ All Go modules built!"

clean-go:
	@echo "🧹 Cleaning all Go modules..."
	@for module in $(MODULES); do \
		echo "Cleaning $$module..."; \
		$(MAKE) -C $$module clean; \
	done
	@echo "✅ All Go modules cleaned!"

test-go:
	@echo "🧪 Testing all Go modules..."
	@for module in $(MODULES); do \
		echo "Testing $$module..."; \
		$(MAKE) -C $$module test; \
	done
	@echo "✅ All Go modules tested!"

install-go:
	@echo "📦 Installing all Go modules..."
	@for module in $(MODULES); do \
		echo "Installing $$module..."; \
		$(MAKE) -C $$module install; \
	done
	@echo "✅ All Go modules installed!"

# Module-specific targets
run-blob-monitor:
	@echo "🔍 Starting blob monitor with example configuration..."
	cd pipeline/blob-monitor && go run cmd/blob-monitor/main.go config.yaml.example

test-blob-monitor:
	@echo "🧪 Testing blob monitor module..."
	cd pipeline/blob-monitor && go test ./...

run-ingest:
	@echo "📥 Running ingestion pipeline..."
	@echo "⚠️  Make sure these environment variables are set:"
	@echo "   SUBSCRIPTION_ID, ENVIRONMENT, AZURE_STORAGE_CONTAINER_NAME"
	@echo "   AZURE_STORAGE_BLOB_NAME, KAFKA_BROKERS, KAFKA_TOPIC"
	cd pipeline/ingest && go run .

# Convenience aliases
build: build-go
test: test-go

# Development mode - just Kafka and Kafdrop (run apps from host)
dev-up:
	@echo "🚀 Starting development infrastructure..."
	docker compose up -d kafka kafdrop
	@echo "✅ Development infrastructure running:"
	@echo "   - Kafka: localhost:9092"
	@echo "   - Kafdrop UI: http://localhost:9000"
	@echo ""
	@echo "💡 Now run your CLI/pipeline from host:"
	@echo "   cd cli && source venv/bin/activate && list-blobs --env P1"
	@echo "   cd pipeline/ingest && go run ."

dev-down:
	@echo "🛑 Stopping development infrastructure..."
	docker compose down

# Build ingestion pipeline image for production testing
build-ingest:
	@echo "🔨 Building ingestion pipeline image..."
	docker build -f pipeline/ingest/Dockerfile -t commerce-logs-ingest:latest .

# Run ingestion pipeline in container (for testing production image)
test-ingest:
	@if [ -z "$(BLOB)" ]; then \
		echo "❌ Error: BLOB parameter required"; \
		echo "Usage: make test-ingest BLOB=path/to/blob.gz"; \
		exit 1; \
	fi
	@echo "🔄 Testing ingestion pipeline in container..."
	docker compose -f docker-compose.yml -f docker-compose.app.yml run --rm \
		-e AZURE_STORAGE_BLOB_NAME=$(BLOB) \
		ingestion-pipeline

# Data management commands
wipe-topics:
	@echo "🧹 Deleting all Kafka topics (keeping containers running)..."
	@if docker ps | grep -q kafka; then \
		echo "📋 Current topics:"; \
		docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list || echo "   (no topics or Kafka not ready)"; \
		echo ""; \
		echo "🗑️  Deleting topics..."; \
		docker exec kafka sh -c 'kafka-topics --bootstrap-server localhost:9092 --list | grep -v "^__" | xargs -r -I {} kafka-topics --bootstrap-server localhost:9092 --delete --topic {}' || echo "   (no user topics to delete)"; \
		echo "✅ Topics deleted. New topics will be auto-created when needed."; \
	else \
		echo "❌ Kafka container not running. Start with: make dev-up"; \
	fi

wipe-data:
	@echo "🧹 Removing all Kafka data (hard reset)..."
	@echo "⚠️  This will stop Kafka, remove all data, and restart it."
	@read -p "Continue? (y/N): " confirm && [ "$$confirm" = "y" ] || { echo "Cancelled."; exit 1; }
	@echo "🛑 Stopping Kafka..."
	docker compose stop kafka
	@echo "🗑️  Removing data volume..."
	docker volume rm commerce-logs-pipeline_kafka-data 2>/dev/null || echo "   (volume already removed or not found)"
	@echo "🚀 Restarting Kafka..."
	docker compose up -d kafka
	@echo "✅ Kafka data wiped and service restarted."
	@echo "   Waiting for Kafka to be ready..."
	@sleep 5
	@docker compose up -d kafdrop
	@echo "✅ Fresh Kafka environment ready!"

list-topics:
	@echo "📋 Current Kafka topics:"
	@if docker ps | grep -q kafka; then \
		docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list --exclude-internal || echo "❌ Kafka not ready or no topics"; \
	else \
		echo "❌ Kafka container not running. Start with: make dev-up"; \
	fi

# Utility commands
logs:
	docker compose logs -f

test-kafka:
	@echo "🔍 Testing Kafka connectivity..."
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list || \
		echo "❌ Kafka not running or not accessible"

clean:
	@echo "🧹 Cleaning up containers and images..."
	docker compose -f docker-compose.yml -f docker-compose.app.yml down --volumes --rmi all
	docker system prune -f 