package com.offsetzero.kafka;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public final class SimpleShareKafkaConsumer {

    public static void main(String[] args) {
        String topic = "single-partitioned-events";

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "offsetzero-share-consumer-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");
        properties.put(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");
        properties.put(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "record_limit");
        
        KafkaShareConsumer<String, String> consumer = new KafkaShareConsumer<>(properties);
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown signal recieved");
            consumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        
        try{
            consumer.subscribe(List.of(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for(ConsumerRecord<String, String> record: records){
                    process(record);
                    consumer.acknowledge(record, AcknowledgeType.ACCEPT);
                    Thread.sleep(1000);
                }
                consumer.commitSync();
                System.out.println("Committed for batch of records "+records.count());
                Thread.sleep(5);
            }
        }catch(WakeupException ex){
            consumer.commitSync();
            System.out.println("Wakeup Exception caught, consumer shutdown initiated");
        }catch(Exception ex){
            System.err.println("Error closing consumer: " + ex.getMessage());
        }finally{
            consumer.close();
            System.out.println("Consumer closed");
        }
    }

    private static void process(ConsumerRecord<String,String> record) {
        System.out.println("Processing record key="+record.key()+" value="+record.value()+" partition="+record.partition());
    }
}
