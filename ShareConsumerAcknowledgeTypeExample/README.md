# Share Consumer Acknowledge Type Example

This example extends `SimpleShareKafkaConsumer` with a typed JSON value
deserializer and explicit acknowledgement types. Each valid event is processed
by `restBackendProcess()`.
If REST processing is throttled, the record is acknowledged with `RELEASE` so
Kafka can make it available for redelivery. A
successfully processed record is acknowledged with `ACCEPT`. If the value
deserializer cannot parse a record, the consumer catches
`RecordDeserializationException`, prints the record metadata, and acknowledges
the record with `REJECT` so the malformed record is not retried.

## Acknowledgement Types

With `share.acknowledgement.mode=explicit`, each acquired record must receive
an acknowledgement before the batch is committed:

| Acknowledgement | Usage in this example |
| --- | --- |
| `ACCEPT` | REST processing completed successfully. The record is complete. |
| `RELEASE` | REST processing was throttled. The record is returned for possible redelivery. |
| `REJECT` | JSON deserialization failed. The malformed record is permanently rejected. |
| `RENEW` | Not used by the current implementation. It is used when processing must extend a record lock before a final acknowledgement. |

After processing the records in a poll, the consumer calls `commitSync()` to
send the pending acknowledgements to Kafka.

## Create The Topic

Create `events` with more than one partition so multiple share-consumer
instances can process records concurrently:

```bash
kafka-topics.sh \
	--bootstrap-server localhost:9092 \
	--create \
	--if-not-exists \
	--topic events \
	--partitions 3 \
	--replication-factor 1
```

Use a replication factor supported by your cluster if it is greater than one
broker. Kafka share groups require a Kafka 4.x broker with share groups enabled.

## Produce Test Records

From this module directory, run the producer script located at the repository
level:

```bash
../../scripts/produce-json-events.sh
```

The script produces 10 valid JSON records and three plain-string records. The
plain strings intentionally demonstrate the `REJECT` deserialization path.
Set `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, or
`KAFKA_CONSOLE_PRODUCER_COMMAND` to override the script defaults.

## Run The Consumer

From this module directory:

```bash
mvn compile exec:java
```

Run multiple instances with the same group ID to observe records being shared
across consumers. The group ID and topic are configured in
`ShareConsumerAcknowledgeTypeExample`.

## Consumer Properties

The Java consumer configures these properties:

| Property | Value | Purpose |
| --- | --- | --- |
| `bootstrap.servers` | `localhost:9092` | Kafka broker address. |
| `group.id` | `offsetzero-acknowledge-type-example` | Share-group identity. Instances with this value share records. |
| `key.deserializer` | `StringDeserializer` | Converts record keys to strings. |
| `value.deserializer` | `EventMessageDeserializer` | Converts JSON object values into `EventMessage` POJOs. |
| `max.poll.records` | `5` | Limits the number of records returned by one poll. |
| `share.acknowledgement.mode` | `explicit` | Requires the application to acknowledge each record. |
| `share.acquire.mode` | `record_limit` | Acquires records according to the configured record limit. |
| `share.record.lock.duration.ms` | `10000` | Duration for which an acquired record remains locked while being processed. |
| `share.auto.offset.reset` | `earliest` | Documents the desired initial offset policy for the share group. Configure the effective group setting on the broker when required. |

The consumer polls every 500 milliseconds, processes each record, acknowledges
it, and commits the batch with `commitSync()`.