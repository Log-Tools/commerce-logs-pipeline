# Blob Structure and Naming Technical Reference

## Blob Naming Convention

Blobs follow a structured naming pattern:
```
kubernetes/YYYYMMDD.{service-name}-{pod-instance}_{namespace}_{container-type}-{hash}.gz
```

### Components Breakdown

- **`kubernetes/`** - Fixed prefix for all log blobs
- **`YYYYMMDD`** - Date in ISO format (e.g., `20250501`)
- **`service-name`** - Service identifier (e.g., `apache2-igc`, `api-service`)
- **`pod-instance`** - Pod-specific components: replicaset hash + pod suffix (e.g., `6ff8f94bbc-4xp2s`)
- **`namespace`** - Kubernetes namespace (typically `default`)
- **`container-type`** - Kubernetes container function within the pod (e.g., `proxy`, `cache-cleaner`, `api`)
- **`hash`** - Unique Kubernetes container hash for identification

### Example Breakdown
```
kubernetes/20250501.apache2-igc-6ff8f94bbc-4xp2s_default_proxy-14ae1767c1692...gz
                    ^^^^^^^^^^^ ^^^^^^^^^^^^^^
                    service     pod instance
                    name        (replicaset + suffix)
```

## Real Example Blob Names

```
kubernetes/20250501.apache2-igc-6ff8f94bbc-4xp2s_default_proxy-14ae1767c16920067f2c545fba7d46c4c9382d3cba1e425f46efd6ca653d3372.gz
kubernetes/20250501.apache2-igc-6ff8f94bbc-4xp2s_default_cache-cleaner-a21d928d37b9c502071416519b87572d19fb3e34495840f9df7da2ed4ebc4c2e.gz
kubernetes/20250501.apache2-igc-nat-574755dfcd-n4m67_default_cache-cleaner-e63fff65728f8db18424f4a36ce63aa84e28a9f8e99c387e930d5ddc6ee51ff9.gz
```

## Service Identification Patterns

### Service Name Extraction for Filtering
The `service-name` component is used functionally for blob filtering:

- **`apache2-igc`**: Identifies Apache/proxy service blobs
- **`api`**: Identifies API service blobs  
- **`backoffice`**: Identifies backoffice service blobs

### Common Service Patterns

#### API Service
- **Pattern**: Contains `api` in container-type or service-name
- **Example**: `kubernetes/20250501.api-abc123_default_api-xyz.gz`

#### Proxy Service (Load Balancer)
- **Pattern**: Contains `proxy` in container-type, often with `apache2-igc` service name
- **Example**: `kubernetes/20250501.apache2-igc-6ff8f94bbc-4xp2s_default_proxy-hash.gz`

#### Cache Cleaner (Infrastructure)
- **Pattern**: Contains `cache-cleaner` in container-type
- **Example**: `kubernetes/20250501.apache2-igc-6ff8f94bbc-4xp2s_default_cache-cleaner-hash.gz`

#### Backoffice Service
- **Pattern**: Contains `backoffice` in service-name or container-type
- **Example**: `kubernetes/20250501.backoffice-deployment-xyz_default_backoffice-hash.gz`

## Important Notes

- **Container Reference**: The blob names reference Kubernetes containers (Docker containers running in pods) but the blob storage structure itself is purely Azure Blob Storage organization
- **No Direct Relationship**: Blob storage hierarchy has no direct relationship to Docker container hierarchies
- **Compressed Format**: All log files are compressed as gzip (`.gz`) files
- **Daily Organization**: Blobs are organized by date for efficient temporal filtering 