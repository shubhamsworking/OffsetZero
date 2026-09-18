# Kafka consumers

This module contains two consumer implementations:

- `LegacyKafkaConsumer`: polls and processes records on one thread with Kafka auto-commit enabled.
- `QueueKafkaConsumer`: uses Kafka 4.x `KafkaShareConsumer`, where consumers in the same share group share records from the same topic. It uses explicit acknowledgements.

## Prerequisites

- Java 17+
- Maven 3.9+
- Kafka running on `localhost:9092`
- A topic named `events`, or set `KAFKA_TOPIC`

## Run

From this directory:

```bash
mvn compile
mvn exec:java -Dexec.mainClass=com.offsetzero.kafka.LegacyKafkaConsumer
mvn exec:java -Dexec.mainClass=com.offsetzero.kafka.QueueKafkaConsumer
```

Both consumers accept these environment variables:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=events
KAFKA_GROUP_ID=offsetzero-consumer
```

The share consumer is a Kafka 4.x preview feature introduced by KIP-932 and requires a Kafka 4.x broker with share groups enabled. `RELEASE` leaves a failed record eligible for another delivery attempt; use `REJECT` for permanently invalid records.
