#!/usr/bin/env bash

set -euo pipefail

bootstrap_servers="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
topic="${KAFKA_TOPIC:-renew-events}"
producer_command="${KAFKA_CONSOLE_PRODUCER_COMMAND:-kafka-console-producer.sh}"

{
    for number in $(seq 1 10); do
        printf '{"id":"event-%03d","event":"created"}\n' "$number"
    done
} | "$producer_command" \
    --bootstrap-server "$bootstrap_servers" \
    --topic "$topic" \
    --command-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner