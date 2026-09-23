# Share Consumer Acknowledge Type Example

This example extends `SimpleShareKafkaConsumer` with a typed JSON value
deserializer and explicit acknowledgement types. JSON values with
`"type":"native"` use `backendProcess()`. Values with `"type":"rest"` use `restBackendProcess()`, which randomly simulates REST
throttling: throttled records are acknowledged with `RELEASE` and successful
records with `ACCEPT`.

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
./scripts/produce-events.sh
```