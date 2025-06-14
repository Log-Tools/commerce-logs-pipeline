# OpenSearch Deployment

This directory contains a minimal setup for running OpenSearch locally for
pipeline development and testing.

## Components

- **Dockerfile** – extends the official `opensearchproject/opensearch` image.
- **docker-compose.yml** – spins up an OpenSearch container and a small
  init container that uploads the index template using
  `configs/opensearch/traces_index_mapping.json`.

## Usage

```bash
# Start OpenSearch and apply the mapping
docker compose -f deployments/opensearch/docker-compose.yml up
```

The init container runs once to create the `traces` index template.
You can also upload the mapping manually using the `opensearch-init` tool located in `tools/opensearch-init`.
You can then index documents following the mapping in `configs/opensearch`.
