# Commerce Cloud Logs Synchronization

## Objective

This document outlines the architecture and concepts for synchronizing logs from commerce cloud environments to support monitoring, debugging, and operational activities.

## Key Concepts

### Subscription
- **Definition**: Top-level organizational unit in commerce cloud
- **Contains**: Multiple environments for different deployment stages
- **Purpose**: Provides logical grouping and resource management boundaries

### Environment
- **Types**: Production (prod), Staging (stage), Development (dev)
- **Naming Convention**: Environments are identified by codes such as:
  - `D1`, `D2`, etc. - Development environments
  - `S1`, `S2`, etc. - Staging environments  
  - `P1`, `P2`, etc. - Production environments
- **Isolation**: Each environment operates independently with its own resources

### Services
Commerce cloud environments contain two categories of services:

#### Commerce Services
- **API Service**: Handles REST API requests and responses
- **Backoffice Service**: Manages administrative interface operations
- **Background Processing Service**: Executes asynchronous tasks and batch jobs
- **Update Service**: Handles deployment and update operations

#### Infrastructure Services
- **Load Balancer (LB)**: Distributes traffic across service instances
- **Database Services**: Manages data persistence layers
- **Other Platform Services**: Supporting infrastructure components

### Container Infrastructure

#### Pods
- **Definition**: Kubernetes pods that host one or more Docker containers
- **Relationship**: Each service runs in one or more pods for scalability and availability
- **Management**: Orchestrated by the underlying Kubernetes platform

#### Containers (Kubernetes/Docker)
- **Function**: Execute the actual service workloads as Docker containers within Kubernetes pods
- **Log Generation**: Each Docker container produces application and system logs
- **Lifecycle**: Containers may be created, destroyed, and recreated during normal operations
- **Note**: These are Docker containers managed by Kubernetes, not Azure storage containers

## Log Storage Architecture

### Azure Blob Storage
- **Storage Location**: Container logs are stored in Azure Blob Storage
- **Organization**: Logs are organized by container, service, and timestamp
- **Retention**: Configurable retention policies based on environment and compliance requirements

### Storage Account Access
- **Access Method**: Each container's logs are stored in a dedicated storage account
- **Authentication**: Storage accounts are accessible using shared access keys
- **Security**: Keys provide controlled access to log data for authorized systems and personnel

## Blob Organization

### Azure Storage Containers
Each environment contains a standard Azure storage container (logical storage unit):
- **Container Name**: `commerce-logs-separated`
- **Purpose**: Contains all log files from the Kubernetes cluster
- **Format**: Compressed gzip files (`.gz`)
- **Note**: This is an Azure Blob Storage container - a logical grouping of blobs, not related to Docker containers

### Blob Structure
- **Naming Pattern**: Blobs follow a structured naming convention that includes date, service information, and Kubernetes metadata
- **Service Identification**: Service names can be extracted from blob paths for filtering
- **Temporal Organization**: Logs are organized by date for efficient retrieval

## Blob Selector

### Definition
A **Blob Selector** is a filtering mechanism that combines a service name with a predicate function to programmatically identify and filter which blobs belong to a specific service.

### Purpose
- **Service Identification**: Map blobs to logical services based on naming patterns
- **Conflict Resolution**: Handle cases where service names alone are ambiguous
- **Sidecar Filtering**: Distinguish main service logs from auxiliary container logs
- **Flexible Matching**: Support complex filtering scenarios beyond simple string matching

### Use Cases
- **Sidecar Container Disambiguation**: Filter out auxiliary containers (cache-cleaner, log-forwarder, monitoring agents)
- **Service Name Conflicts**: Use additional context for accurate service identification


