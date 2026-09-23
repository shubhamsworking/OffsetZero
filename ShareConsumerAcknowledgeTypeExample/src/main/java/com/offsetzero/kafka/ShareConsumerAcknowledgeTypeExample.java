package com.offsetzero.kafka;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public final class ShareConsumerAcknowledgeTypeExample {

    private static final String TOPIC = "events";

    private ShareConsumerAcknowledgeTypeExample() {
    }

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "offsetzero-acknowledge-type-example");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EventMessageDeserializer.class.getName());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");
        properties.put(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");
        properties.put("share.acquire.mode", "record_limit");
        properties.put("share.record.lock.duration.ms", "10000");
        properties.put("share.auto.offset.reset", "earliest");

        KafkaShareConsumer<String, EventMessage> consumer = new KafkaShareConsumer<>(properties);
        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown signal received");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }));

        try {
            consumer.subscribe(List.of(TOPIC));
            while (true) {
                ConsumerRecords<String, EventMessage> records;
                try {
                    records = consumer.poll(Duration.ofMillis(500));
                } catch (RecordDeserializationException exception) {
                    rejectDeserializationFailure(consumer, exception);
                    consumer.commitSync();
                    continue;
                }

                for (ConsumerRecord<String, EventMessage> record : records) {
                    if ("rest".equalsIgnoreCase(record.value().getType())) {
                        restBackendProcess(record, consumer);
                    } else {
                        backendProcess(record, consumer);
                    }
                }
                consumer.commitSync();
                System.out.println("Committed for batch of records " + records.count());
            }
        } catch (WakeupException exception) {
            consumer.commitSync();
            System.out.println("Wakeup exception caught, consumer shutdown initiated");
        } catch (Exception exception) {
            System.err.println("Error closing consumer: " + exception.getMessage());
        } finally {
            consumer.close();
            System.out.println("Consumer closed");
        }
    }

    private static void backendProcess(ConsumerRecord<String, EventMessage> record,
                                       KafkaShareConsumer<String, EventMessage> consumer) {
        System.out.println("backendProcess record metadata: topic=" + record.topic()
                + ", partition=" + record.partition() + ", offset=" + record.offset()
                + ", value=" + record.value());
        consumer.acknowledge(record, AcknowledgeType.ACCEPT);
    }

    private static void restBackendProcess(ConsumerRecord<String, EventMessage> record,
                                            KafkaShareConsumer<String, EventMessage> consumer) {
        if (ThreadLocalRandom.current().nextBoolean()) {
            System.out.println("REST backend throttled record topic=" + record.topic()
                    + ", partition=" + record.partition() + ", offset=" + record.offset()
                    + "; releasing it for redelivery");
            consumer.acknowledge(record, AcknowledgeType.RELEASE);
            return;
        }

        System.out.println("REST backend processed record topic=" + record.topic()
            + ", partition=" + record.partition() + ", offset=" + record.offset()
            + ", value=" + record.value());
        consumer.acknowledge(record, AcknowledgeType.ACCEPT);
    }

    private static void rejectDeserializationFailure(KafkaShareConsumer<String, EventMessage> consumer,
                                                     RecordDeserializationException exception) {
        String topic = exception.topicPartition().topic();
        int partition = exception.topicPartition().partition();
        long offset = exception.offset();
        System.err.println("Rejecting deserialization failure topic=" + topic
                + ", partition=" + partition + ", offset=" + offset
                + ": " + exception.getMessage());
        consumer.acknowledge(topic, partition, offset, AcknowledgeType.REJECT);
    }
}