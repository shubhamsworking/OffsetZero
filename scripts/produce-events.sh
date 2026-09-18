#!/usr/bin/env bash

set -euo pipefail

bootstrap_servers="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
topic="${KAFKA_TOPIC:-single-partitioned-events}"
producer_command="${KAFKA_CONSOLE_PRODUCER_COMMAND:-kafka-console-producer.sh}"

seq 1 100 | awk '{ printf "event-%03d\n", $1 }' | "$producer_command" \
    --bootstrap-server "$bootstrap_servers" \
    --topic "$topic" \
    --command-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner