# Working with Kafka without Docker

If Docker is not available, the usual `make dev-up` workflow cannot start Kafka. Instead run a local broker manually.

The broker listens on `localhost:9092`. Check `kafka.log` for startup progress.

## Verifying Connectivity
- Test with the pipeline tool:
  ```bash
  cd pipeline/ingest
  go run . test-kafka
  ```
- Or list topics directly:
  ```bash
  $KAFKA_DIR/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
  ```
