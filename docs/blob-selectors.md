# Blob Selectors Implementation Guide

## Definition

A **Blob Selector** is a functional filtering mechanism that combines a service name with a predicate function to programmatically identify and filter which blobs belong to a specific service.

## Components

```python
BlobSelector {
    service_name: String,           // Target service name for filtering
    predicate: (blob_name: String) -> Boolean  // Functional filter logic
}
```

## Implementation Pattern

```python
from typing import Callable, List

class BlobSelector:
    def __init__(self, service_name: str, predicate: Callable[[str], bool]):
        self.service_name = service_name
        self.predicate = predicate
    
    def matches(self, blob_name: str) -> bool:
        return self.predicate(blob_name)
    
    def filter_blobs(self, blobs: List[str]) -> List[str]:
        return [blob for blob in blobs if self.matches(blob)]
```

## Functional Usage

Blob selectors are used by the log synchronization system to:

1. **Parse service names** from blob paths (e.g., extract `apache2-igc` from blob names)
2. **Apply primary filter** based on service name matching
3. **Apply secondary predicates** for additional filtering logic
4. **Return filtered blob lists** for specific services

## Examples

### Simple Service Selector

```python
api_selector = BlobSelector(
    service_name="API Service",
    predicate=lambda name: "_api-" in name or "api-service" in name
)
```

### Proxy Service with Date Filter

```python
proxy_selector = BlobSelector(
    service_name="Load Balancer",
    predicate=lambda name: "_proxy-" in name and name.startswith("kubernetes/20250501")
)
```

### Complex Selector with Multiple Conditions

```python
background_selector = BlobSelector(
    service_name="Background Processing",
    predicate=lambda name: (
        "background" in name and 
        "cache-cleaner" not in name and  # Exclude sidecar
        name.endswith(".gz")
    )
)
```

### Exclude Sidecar Containers

```python
commerce_api_selector = BlobSelector(
    service_name="Commerce API",
    predicate=lambda name: (
        "commerce" in name and 
        "_api-" in name and
        "cache-cleaner" not in name and    # Exclude cache sidecar
        "log-forwarder" not in name        # Exclude logging sidecar
    )
)
```

### Apache Proxy Service Selector

```python
apache_proxy_selector = BlobSelector(
    service_name="Apache Proxy",
    predicate=lambda name: (
        "apache2-igc" in name and 
        "_proxy-" in name and
        "cache-cleaner" not in name  # Exclude sidecar
    )
)
```

## Use Case Implementations

### 1. Sidecar Container Disambiguation

When main service pods include sidecar containers, use exclusion patterns:

```python
def create_main_service_selector(service_pattern: str) -> BlobSelector:
    return BlobSelector(
        service_name=f"Main {service_pattern} Service",
        predicate=lambda name: (
            service_pattern in name and
            "cache-cleaner" not in name and
            "log-forwarder" not in name and
            "monitoring" not in name
        )
    )
```

### 2. Time-based Filtering

Include temporal constraints:

```python
def create_date_filtered_selector(service_pattern: str, date: str) -> BlobSelector:
    return BlobSelector(
        service_name=f"{service_pattern} ({date})",
        predicate=lambda name: (
            service_pattern in name and
            name.startswith(f"kubernetes/{date}")
        )
    )
```

### 3. Environment-specific Logic

Handle different naming conventions:

```python
def create_environment_aware_selector(service_pattern: str, environment: str) -> BlobSelector:
    if environment.startswith('P'):  # Production
        predicate = lambda name: service_pattern in name and "prod" in name
    elif environment.startswith('S'):  # Staging
        predicate = lambda name: service_pattern in name and "staging" in name
    else:  # Development
        predicate = lambda name: service_pattern in name
    
    return BlobSelector(
        service_name=f"{service_pattern} ({environment})",
        predicate=predicate
    )
```

## Common Service Selectors

### Pre-defined Selectors for Commerce Services

```python
# Common selectors for known services
COMMON_SELECTORS = {
    "apache-proxy": BlobSelector(
        "Apache Proxy",
        lambda name: "apache2-igc" in name and "_proxy-" in name
    ),
    
    "cache-cleaner": BlobSelector(
        "Cache Cleaner",
        lambda name: "_cache-cleaner-" in name
    ),
    
    "api-service": BlobSelector(
        "API Service", 
        lambda name: "api" in name and "_api-" in name
    ),
    
    "backoffice": BlobSelector(
        "Backoffice Service",
        lambda name: "backoffice" in name
    )
}
```

## Usage Example

```python
# Filter blobs for Apache proxy service
blobs = azure_client.list_blobs('cp2', 'D1', 'commerce-logs-separated')
proxy_blobs = COMMON_SELECTORS["apache-proxy"].filter_blobs([blob['name'] for blob in blobs])

print(f"Found {len(proxy_blobs)} proxy service blobs")
for blob in proxy_blobs[:5]:
    print(f"  - {blob}")
```

This selector-based approach provides flexibility for identifying service logs across different commerce cloud deployments and configurations. 