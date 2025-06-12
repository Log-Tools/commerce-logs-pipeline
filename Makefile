# Commerce Logs Pipeline - Docker Operations & Go Builds
.PHONY: help dev-up dev-down build test clean build-go clean-go test-go install-go status quick-start

# Go modules
MODULES := pipeline/blob-monitor pipeline/events pipeline/ingest

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
	@echo "  make quick-start     Complete fresh setup (stop→clean→start→init)"
	@echo "  make dev-up          Start Kafka & Kafdrop for local development"
	@echo "  make dev-down        Stop development services"
	@echo "  make status          Show system health and running services"
	@echo ""
	@echo "Production testing:"
	@echo "  make build-ingest    Build ingestion pipeline image"
	@echo "  make test-ingest     Run ingestion pipeline in container"
	@echo ""
	@echo "Data management:"
	@echo "  make wipe-topics     Delete all Kafka topics (soft reset)"
	@echo "  make wipe-data       Remove all Kafka data (hard reset)"
	@echo "  make list-topics     Show current Kafka topics"
	@echo "  make list-active-blobs Show currently active (open) blobs from compacted state"
	@echo ""
	@echo "Utilities:"
	@echo "  make logs           Show logs from all services"
	@echo "  make clean          Remove all containers and images"
	@echo "  make init-kafka      Create/verify Kafka topics from configs/kafka_topics.yaml"
	@echo "  make test-kafka     Test Kafka connectivity"

# Go build targets
build-go:
	@echo "🔨 Building all Go modules..."
	@for module in $(MODULES); do \
		echo "Building $$module..."; \
		/usr/bin/make -C $$module build; \
	done
	@echo "✅ All Go modules built!"

clean-go:
	@echo "🧹 Cleaning all Go modules..."
	@for module in $(MODULES); do \
		echo "Cleaning $$module..."; \
		/usr/bin/make -C $$module clean; \
	done
	@echo "✅ All Go modules cleaned!"

test-go:
	@echo "🧪 Testing all Go modules..."
	@for module in $(MODULES); do \
		echo "Testing $$module..."; \
		/usr/bin/make -C $$module test; \
	done
	@echo "✅ All Go modules tested!"

install-go:
	@echo "📦 Installing all Go modules..."
	@for module in $(MODULES); do \
		echo "Installing $$module..."; \
		/usr/bin/make -C $$module install; \
	done
	@echo "✅ All Go modules installed!"

# Module-specific targets
run-blob-monitor:
	@echo "🔍 Starting blob monitor with example configuration..."
	cd pipeline/blob-monitor && go run cmd/blob-monitor/main.go configs/config.yaml

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
	@echo "⏳ Waiting for Kafka to be ready..."
	@sleep 8
	@$(MAKE) init-kafka
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
		TOPICS=$$(docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -v "^__" | grep -v "^$$"); \
		if [ -n "$$TOPICS" ]; then \
			for topic in $$TOPICS; do \
				echo "   Deleting topic: $$topic"; \
				docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic "$$topic"; \
			done; \
		else \
			echo "   (no user topics to delete)"; \
		fi; \
		echo "✅ Topics deleted. New topics will be auto-created when needed."; \
		echo "🔄 Recreating topics with proper configuration..."; \
		$(MAKE) init-kafka; \
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
	@echo "⏳ Waiting for Kafka to be ready..."
	@sleep 8
	@echo "🛠️  Initializing topics..."
	@$(MAKE) init-kafka
	@echo "✅ Fresh Kafka environment ready!"
	@sleep 2
	@docker compose up -d kafdrop
	@echo "🌐 Kafdrop UI available at http://localhost:9000"

list-topics:
	@echo "📋 Current Kafka topics:"
	@if docker ps | grep -q kafka; then \
		docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list --exclude-internal || echo "❌ Kafka not ready or no topics"; \
	else \
		echo "❌ Kafka container not running. Start with: make dev-up"; \
	fi

# Lists currently active blobs (status = "open") from the compacted BlobState topic
list-active-blobs:
	@echo "📋 Currently active (open) blobs:"
	@if docker ps | grep -q kafka; then \
		docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic Ingestion.BlobState --from-beginning --timeout-ms 5000 --property print.key=true --property key.separator='|' 2>/dev/null \
		| awk -F '|' '{state[$$1]=$$2} END {for (k in state) if (state[k] ~ /\"status\":\"open\"/) print k}' \
		| sort; \
	else \
		echo "❌ Kafka container not running. Start with: make dev-up"; \
	fi

# Utility commands
logs:
	docker compose logs -f

test-kafka:
	@echo "🔍 Testing Kafka connectivity..."
	@if docker ps | grep -q kafka; then \
		docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && \
		echo "✅ Kafka is accessible" || \
		echo "❌ Kafka not ready or not accessible"; \
	else \
		echo "❌ Kafka container not running. Start with: make dev-up"; \
	fi

clean:
	@echo "🧹 Cleaning up containers and images..."
	docker compose -f docker-compose.yml -f docker-compose.app.yml down --volumes --rmi all
	docker system prune -f

# Initializes required Kafka topics based on configs/kafka_topics.yaml
init-kafka:
	@echo "🛠️  Initializing Kafka topics..."
	@if docker ps | grep -q kafka; then \
		echo "⏳ Waiting for Kafka to be fully ready..."; \
		timeout=30; \
		while [ $$timeout -gt 0 ]; do \
			if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then \
				echo "✅ Kafka is ready"; \
				break; \
			fi; \
			echo "   Waiting... ($$timeout seconds remaining)"; \
			sleep 2; \
			timeout=$$((timeout-2)); \
		done; \
		if [ $$timeout -le 0 ]; then \
			echo "❌ Kafka failed to become ready within 30 seconds"; \
			exit 1; \
		fi; \
		echo "📋 Creating/verifying topics from configs/kafka_topics.yaml..."; \
		cd tools/kafka-init && go run . -brokers localhost:9092 -config ../../configs/kafka_topics.yaml; \
		echo "✅ Kafka topics initialized"; \
	else \
		echo "❌ Kafka container not running. Start with: make dev-up"; \
		exit 1; \
	fi

# Enhanced status command to show full system health
status:
	@echo "🔍 System Status Check"
	@echo "====================="
	@echo ""
	@echo "📦 Docker Services:"
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "   No services running"
	@echo ""
	@echo "📋 Kafka Topics:"
	@if docker ps | grep -q kafka; then \
		if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then \
			docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list --exclude-internal | sed 's/^/   /' || echo "   (no topics)"; \
		else \
			echo "   ❌ Kafka not ready"; \
		fi; \
	else \
		echo "   ❌ Kafka not running"; \
	fi
	@echo ""
	@echo "🌐 Web Interfaces:"
	@if docker ps | grep -q kafdrop; then \
		echo "   ✅ Kafdrop UI: http://localhost:9000"; \
	else \
		echo "   ❌ Kafdrop not running"; \
	fi

# Quick development setup command
quick-start:
	@echo "🚀 Quick Development Setup"
	@echo "=========================="
	@echo ""
	@echo "🛑 Stopping any existing services..."
	@docker compose down >/dev/null 2>&1 || true
	@echo "🧹 Cleaning old data..."
	@docker volume rm commerce-logs-pipeline_kafka-data >/dev/null 2>&1 || true
	@echo "🚀 Starting fresh environment..."
	@docker compose up -d kafka kafdrop
	@echo "⏳ Waiting for Kafka to be ready..."
	@sleep 8
	@echo "🛠️  Initializing Kafka topics..."
	@if docker ps | grep -q kafka; then \
		echo "⏳ Waiting for Kafka to be fully ready..."; \
		timeout=30; \
		while [ $$timeout -gt 0 ]; do \
			if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then \
				echo "✅ Kafka is ready"; \
				break; \
			fi; \
			echo "   Waiting... ($$timeout seconds remaining)"; \
			sleep 2; \
			timeout=$$((timeout-2)); \
		done; \
		if [ $$timeout -le 0 ]; then \
			echo "❌ Kafka failed to become ready within 30 seconds"; \
			exit 1; \
		fi; \
		echo "📋 Creating/verifying topics from configs/kafka_topics.yaml..."; \
		cd tools/kafka-init && go run . -brokers localhost:9092 -config ../../configs/kafka_topics.yaml; \
		echo "✅ Kafka topics initialized"; \
	else \
		echo "❌ Kafka container not running"; \
		exit 1; \
	fi
	@echo "✅ Development infrastructure running:"
	@echo "   - Kafka: localhost:9092"
	@echo "   - Kafdrop UI: http://localhost:9000"
	@echo ""
	@echo "🎉 Ready! Your development environment is up and running!"
	@echo "   Run 'make status' to see system health" 