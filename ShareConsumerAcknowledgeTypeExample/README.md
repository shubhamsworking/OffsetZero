# Share Consumer Acknowledge Type Example

This example extends `SimpleShareKafkaConsumer` with a typed JSON value
deserializer and explicit acknowledgement types. JSON values with
each event is processed asynchronously. While the task is incomplete, the consumer sends
`RENEW` every five seconds and commits the renewal. Failed tasks use `RELEASE`;
successful tasks use `ACCEPT`.

The consumer catches `RecordDeserializationException` for the plain-string
records, prints their failed record metadata, acknowledges them with `REJECT`,
and commits them.

## Run

Start Kafka on `localhost:9092`, then run:

```bash
mvn compile exec:java
```

The producer script sends 50 JSON records plus three plain strings:

```bash
./scripts/produce-json-events.sh
```