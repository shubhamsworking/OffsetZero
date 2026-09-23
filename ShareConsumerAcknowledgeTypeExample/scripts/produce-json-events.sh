#!/usr/bin/env bash

set -euo pipefail

bootstrap_servers="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
topic="${KAFKA_TOPIC:-events}"
producer_command="${KAFKA_CONSOLE_PRODUCER_COMMAND:-kafka-console-producer.sh}"

{
    for number in $(seq 1 50); do
        if (( number % 2 == 0 )); then
            type="rest"
        else
            type="native"
        fi
        printf '{"id":"event-%03d","type":"%s","event":"created"}\n' "$number" "$type"
    done
    printf 'not-json-record-1\n'
    printf 'not-json-record-2\n'
    printf 'not-json-record-3\n'
} | "$producer_command" \
    --bootstrap-server "$bootstrap_servers" \
    --topic "$topic" \
    --command-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner