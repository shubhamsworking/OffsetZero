# OffsetZero

OffsetZero is a collection of reusable code and helper definitions for solving
production error cases and implementing practical features. The examples are
intended to provide patterns that can be adapted and reused in real
applications.

## Scripts

Helper Bash scripts produce events in the formats required by the `KafkaShareConsumer` examples. Update the Kafka CLI path to your `KAFKA_HOME/bin` directory and change the topic name as needed.

## SimpleShareKafkaConsumer

Demonstrates a basic implementation of `KafkaShareConsumer`. Use it to explore how a share consumer differs from the legacy `KafkaConsumer`.

## ShareConsumerAcknowledgeTypeExample

Demonstrates a `KafkaShareConsumer` use case where records are acknowledged with different acknowledgement types: `ACCEPT`, `REJECT`, or `RELEASE`.

## ShareConsumerRenewAcknowledgeTypeExample

Demonstrates concurrent record processing with multiple threads using `KafkaShareConsumer`. It shows how to renew record locks and acknowledge records with `RENEW`, `ACCEPT`, `REJECT`, or `RELEASE`.