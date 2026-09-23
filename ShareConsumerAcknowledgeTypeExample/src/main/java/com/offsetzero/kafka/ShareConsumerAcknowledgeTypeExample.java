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
        properties.put(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "record_limit");

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
                    continue;
                }

                for (ConsumerRecord<String, EventMessage> record : records) {
                    Thread.sleep(3000);
                    restBackendProcess(record, consumer);
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

    private static void restBackendProcess(ConsumerRecord<String, EventMessage> record,
                                            KafkaShareConsumer<String, EventMessage> consumer) {
        if (ThreadLocalRandom.current().nextBoolean()) {
            System.out.println("REST backend throttled record id = " + record.value().getId() + "; releasing it for redelivery");
            consumer.acknowledge(record, AcknowledgeType.RELEASE);
            return;
        }

        System.out.println("REST backend processed record id = " +record.value().getId());
        consumer.acknowledge(record, AcknowledgeType.ACCEPT);
    }

    private static void rejectDeserializationFailure(KafkaShareConsumer<String, EventMessage> consumer,
                                                     RecordDeserializationException exception) {
        String topic = exception.topicPartition().topic();
        int partition = exception.topicPartition().partition();
        long offset = exception.offset();
        System.err.println("Rejecting deserialization failure record id =" + topic
                + ", partition=" + partition + ", offset=" + offset
                + ": " + exception.getMessage());
        consumer.acknowledge(topic, partition, offset, AcknowledgeType.REJECT);
    }
}