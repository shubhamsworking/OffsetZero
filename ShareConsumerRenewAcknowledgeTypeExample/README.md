# Kafka Share Consumer: Renew and Acknowledge Example

This example demonstrates using a `KafkaShareConsumer` with multiple processing threads. It shows how to renew a record’s lock while processing takes longer than expected, to avoid submitting the same record for processing more than once within the consumer instance.

## Prerequisites

- Java 17+
- Maven 3.9+
- Kafka 4.x running on `localhost:9092`
- `kafka-topics.sh`, `kafka-configs.sh`, and `kafka-console-producer.sh`
- A Kafka installation with the commands above available, or their full paths

## Recommended walkthrough

1. Start with one consumer instance and a topic with a single partition. This makes the processing flow easier to follow.
2. Create a topic with one partition.
```bash
kafka-topics.sh \
	--bootstrap-server localhost:9092 \
	--create \
	--if-not-exists \
	--topic renew-events \
	--partitions 1 \
	--replication-factor 1
```
3. Start the consumer. Update the topic name in the code or configuration if needed.
4. Use the producer script `produce-json-events.sh` in the `scripts` directory to send JSON events of type `EventMessage`. Update its topic name and Kafka CLI path as needed.
5. Review the consumer logs to follow polling, processing, lock renewal, acknowledgements, and commits.
6. Once the single-consumer flow is clear, try running multiple consumer instances.

## Processing flow

1. Poll the consumer for records.
2. Submit records to worker threads for concurrent processing, skipping records already being processed.
3. Wait up to at most two seconds for processing results.
4. Renew the locks for records that are still processing, so their sessions do not expire during longer work.
5. Acknowledge completed records:
   - `ACCEPT` when processing succeeds.
   - `REJECT` when processing fails with an unrecoverable exception.
6. Remove completed records from the in-flight processing state.
7. Commit manually.
8. Poll again and check for results from records that are still processing. Renew their locks as needed, then acknowledge them when processing completes.
9. Continue with the next batch.

## Notes

- Test with a single-partition topic first; it makes the flow easier to understand.
- Lock renewal helps prevent records from becoming available to another consumer while they are still being processed. In-flight tracking helps prevent duplicate submission within the same consumer instance.
- The `auto.offset.reset` behavior for `KafkaShareConsumer` is configured on the server side, not by the client. Use the Kafka configuration tooling appropriate for your Kafka version and cluster to change it.
```bash
/path/to/kafka/bin/kafka-configs.sh \
	--bootstrap-server localhost:9092 \
	--entity-type groups \
	--entity-name offsetzero-share-consumer-group \
	--add-config share.auto.offset.reset=earliest \
	--alter
```

## Key takeaways

1. as `KafkaShareConsumer` is not thread-safe, `consumer.acknowledge(record, ackType)` needs to be handled by single thread, and can not be done by worker threads processing `ConsumerRecords` concurrently.
2. If a batch includes at least one record acknowledged with `RENEW`, calling `commit()` does not advance to a new batch on the next poll. The `POLL` continues returning records that acknowledged with `RENEW` until they are acknowledged with `ACCEPT` or `REJECT` or `RELEASE`.
3. With explicit acknowledgement enabled, acknowledging every record in the batch before calling `commitSync()` or `commitAsync()` is required. Even if `KafkaShareConsumer` features individual acknowledgement of `ConsumerRecords`, these commit methods do not commit a partially acknowledged batch. 