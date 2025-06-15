# Working with Kafka in this Environment

Docker is not available, so the usual `make dev-up` workflow cannot start Kafka. Instead run a local broker manually.

## Starting Kafka
1. Install Java 17 if not already present:
   ```bash
   sudo apt-get update
   sudo apt-get install -y openjdk-17-jre-headless
   ```
2. Download and extract Kafka 3.7.0:
   ```bash
   curl -L -o kafka.tgz https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz
   tar xzf kafka.tgz
   export KAFKA_DIR="$(pwd)/kafka_2.13-3.7.0"
   ```
3. Format storage for KRaft mode and start the server:
   ```bash
   $KAFKA_DIR/bin/kafka-storage.sh random-uuid > kafka.uuid
   $KAFKA_DIR/bin/kafka-storage.sh format -t $(cat kafka.uuid) -c $KAFKA_DIR/config/kraft/server.properties
   nohup $KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/kraft/server.properties > kafka.log 2>&1 &
   ```
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

## Cleanup
Stop the Kafka process when finished and remove the extracted Kafka directory and logs. Do **not** commit `kafka.tgz`, `kafka.uuid`, or the `kafka_2.13-3.7.0` directory to git.
