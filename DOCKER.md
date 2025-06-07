# Docker Strategy for Commerce Logs Pipeline

This document outlines a pragmatic Docker strategy that containerizes what makes sense and keeps development tools simple.

## 🏗️ Architecture Overview

```
Development → Production Testing → Production
    ↓              ↓                 ↓
Host + Docker → Docker Testing → Kubernetes
```

### Key Principles

1. **Containerize Production Services** - Pipeline components that run in production
2. **Keep Dev Tools Simple** - CLI tools stay on host for ease of use
3. **Configuration Reuse** - Same config format across all environments  
4. **Progressive Deployment** - Easy path from development to production

## 📁 File Structure

```
├── docker-compose.yml           # Infrastructure (Kafka, Kafdrop)
├── docker-compose.app.yml       # Production services (Ingestion pipeline)
├── pipeline/ingest/Dockerfile   # Ingestion pipeline container
├── Makefile                     # Common Docker operations
└── k8s/                         # Kubernetes manifests (future)
    ├── kafka/
    └── ingestion/
```

## 🚀 Deployment Modes

### 1. Development Mode (Recommended)

**What runs where:**
- 🐳 **Docker**: Kafka + Kafdrop (infrastructure)
- 🖥️ **Host**: CLI tools + Ingestion pipeline (your code)

**Benefits:**
- Fast iteration (no rebuild needed)
- Direct debugging with your IDE
- Simple configuration access
- Native performance

**Usage:**
```bash
# Start infrastructure
make dev-up

# Use CLI tools normally
cd cli && source venv/bin/activate
list-blobs --env P1
setup-config

# Run ingestion pipeline
cd pipeline/ingest
go run .
```

### 2. Production Testing Mode

**What runs where:**
- 🐳 **Docker**: Infrastructure + Production services
- 🖥️ **Host**: CLI tools (still for management)

**Benefits:**
- Test production container builds
- Validate deployment configurations
- Same images that will run in Kubernetes

**Usage:**
```bash
# Build production image
make build-ingest

# Test ingestion in container
make test-ingest BLOB="kubernetes/my-blob.gz"

# CLI tools still from host
list-blobs --env P1
```

### 3. Production Mode (Kubernetes)

**What runs where:**
- ☸️ **Kubernetes**: All production services
- 🖥️ **Host/Bastion**: CLI tools for administration

**Benefits:**
- Scalability and orchestration
- Production-grade monitoring
- Service mesh capabilities

## 🤔 Why Not Containerize CLI Tools?

CLI tools are **development and administrative utilities** that work better on the host:

❌ **Containerized CLI Problems:**
- Interactive commands are awkward
- File/config access is complex
- IDE integration breaks
- Debugging becomes harder
- Volume mounting complications

✅ **Host CLI Benefits:**
- Native interactive experience
- Direct file system access
- IDE integration works
- Simple configuration
- Fast execution

**Bottom line:** Containerize services that run in production, keep dev tools simple.

## 🔧 Configuration Strategy

### Same Config Everywhere

```yaml
# ~/.commerce-logs-pipeline/config.yaml (works for all modes)
subscriptions:
  cp2:
    environments:
      P1:
        storage_account:
          account_name: "prod_account"
          access_key: "prod_key"
```

### Environment Variables

**Development:**
```bash
# Run from host
export KAFKA_BROKERS=localhost:9092
go run .
```

**Production Testing:**
```bash
# Container uses internal networking
# KAFKA_BROKERS=kafka:29092 (set in docker-compose)
make test-ingest BLOB=...
```

**Kubernetes:**
```yaml
env:
- name: KAFKA_BROKERS
  value: kafka-service:9092
```

## 📋 Common Workflows

### Daily Development (Recommended)
```bash
# Start infrastructure once
make dev-up

# Work normally with tools
cd cli && source venv/bin/activate
list-blobs --env P1

# Develop and test pipeline
cd pipeline/ingest
go run .

# Stop when done
make dev-down
```

### Data Management
```bash
# Check current topics
make list-topics

# Soft reset - delete topics but keep containers running
make wipe-topics

# Hard reset - remove all data and restart Kafka
make wipe-data
```

### Before Production Deployment
```bash
# Build and test production image
make build-ingest
make test-ingest BLOB="kubernetes/test-blob.gz"

# Deploy to Kubernetes with same image
kubectl apply -f k8s/ingestion/
```

### Administration
```bash
# Always use host CLI tools
list-blobs --env P1 --max-per-env 5
setup-config  # For new environments
```

## 🎯 What to Containerize

✅ **Do Containerize:**
- **Ingestion Pipeline** - Production service
- **Future processors** - Data transformation services  
- **Infrastructure** - Kafka, databases, etc.

❌ **Don't Containerize:**
- **CLI Tools** - Development utilities
- **Build tools** - Stay with host toolchain
- **IDE/Editor** - Obviously stays on host

## 🔄 Migration to Kubernetes

When ready for production, you'll reuse the same container images:

```yaml
# k8s/ingestion/deployment.yaml
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
      - name: ingestion
        image: commerce-logs-ingest:latest  # ← Same image from Docker testing
        env:
        - name: KAFKA_BROKERS
          value: kafka-service:9092
```

**No duplication** - same containers, same configs, proven approach.

## 🎉 Benefits of This Approach

1. **Simple Development** - No container overhead for dev tools
2. **Production Ready** - Services are containerized and tested  
3. **No Duplication** - Same images from testing to production
4. **Developer Friendly** - Fast iteration, normal debugging
5. **Practical** - Containerize what benefits from it

## 🚀 Quick Start

```bash
# 1. Start development infrastructure
make dev-up

# 2. Use CLI tools normally
cd cli && source venv/bin/activate
list-blobs --env P1

# 3. When ready to test production builds
make build-ingest
make test-ingest BLOB="kubernetes/some-blob.gz"
```

This approach gives you the benefits of containerization where it matters while keeping development simple and productive. 