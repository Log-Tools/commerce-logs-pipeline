# opensearch-init

Simple utility to upload the traces index template to an OpenSearch cluster.

```bash
opensearch-init -addr http://localhost:9200 \
  -mapping configs/opensearch/traces_index_mapping.json
```
