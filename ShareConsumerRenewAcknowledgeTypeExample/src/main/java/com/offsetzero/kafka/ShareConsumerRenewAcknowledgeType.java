package com.offsetzero.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ShareConsumerRenewAcknowledgeType {

    private static final String TOPIC = "renew-events";
    //Size = MAX_POLL_RECORDS_CONFIG
    private static final ConcurrentHashMap<ConsumerRecord<String, EventMessage>, Future<Boolean>> underExecutionRecords = new ConcurrentHashMap<>(5);
    //Size <= MAX_POLL_RECORDS_CONFIG for optimized cpu usage
    private static final ExecutorService processExecutorService = Executors.newFixedThreadPool(5);
    
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
                    System.out.println("-------------------------POLLED------------------------------");
                    for (ConsumerRecord<String, EventMessage> record : records) {
                        if(!underExecutionRecords.containsKey(record)){
                            //assign records to different threads for concurrent processing
                            Future<Boolean> futureProcessor = processExecutorService.submit(() -> process(record, consumer));
                            System.out.println(Instant.now()+" Adding record "+record.value().getId()+" to underExecutionRecords");
                            underExecutionRecords.put(record, futureProcessor);
                        }
                    }
                    //collect acknowledgements from childrent threads
                    handleAcknowledgements(underExecutionRecords, consumer);
                } catch (RecordDeserializationException exception) {
                    rejectDeserializationFailure(consumer, exception);
                    continue;
                }
                consumer.commitSync();
                System.out.println(Instant.now()+" Committed for batch of records " + records.count());
            }
        } catch (WakeupException exception) {
            consumer.commitSync();
            System.out.println(Instant.now()+" Wakeup exception caught, consumer shutdown initiated");
        } catch (Exception exception) {
            System.err.println(Instant.now()+" Error closing consumer: " + exception.getMessage());
        } finally {
            processExecutorService.shutdown();
            consumer.close(); 
            System.out.println(Instant.now()+" Consumer closed");
        }
    }

    private static void handleAcknowledgements(
            ConcurrentHashMap<ConsumerRecord<String,EventMessage>,Future<Boolean>> underExecutionRecords, KafkaShareConsumer<String, EventMessage> consumer) {
        
        underExecutionRecords.forEach((record, futureProcessor) -> {
            try {
                    System.out.println(Instant.now()+" Calling future.get() for "+record.value().getId());
                    Boolean isDone = futureProcessor.get(2, TimeUnit.SECONDS);
                    
                    if(isDone){
                        consumer.acknowledge(record, AcknowledgeType.ACCEPT);
                        System.out.println(Instant.now()+" ACCEPT ACK for "+record.value().getId());
                    }else{
                        consumer.acknowledge(record, AcknowledgeType.REJECT);
                        System.out.println(Instant.now()+" REJECT ACK for "+record.value().getId());
                    }
                        
                System.out.println(Instant.now()+" Removing record "+record.value().getId()+" from underExecutionRecords");
                underExecutionRecords.remove(record); //either ACCEPT / REJECT in both cases record won't be polled again hence remove
           
            }catch(TimeoutException e){ 
                //If TimedOut after 2 seconds, it means record is still being processed hence renew the lock 
                consumer.acknowledge(record, AcknowledgeType.RENEW);
                System.out.println(Instant.now()+" Renewed lock for record id "+record.value().getId());
            
            }catch(Exception e){
                //Release record for reattempt as interrupted by unexpected exception
                consumer.acknowledge(record, AcknowledgeType.RELEASE);
                System.out.println(Instant.now()+" Release for record id "+record.value().getId());
                e.printStackTrace();
            }
        });
        
    }

    private static boolean process(ConsumerRecord<String,EventMessage> record, KafkaShareConsumer<String,EventMessage> consumer) {
        Random random = new Random();
        try{
            if(random.nextBoolean()){ // Simulating long process time of 15sec for few records
                System.out.println(Instant.now()+" Long Processing started for record id "+record.value().getId()+" by thread "+Thread.currentThread().getName());
                Thread.sleep(15000);
                System.out.println(Instant.now()+" Long Processing completed for record id "+record.value().getId()+" by thread "+Thread.currentThread().getName());
            }else{ // Simulating short process time of 1sec for few records
                Thread.sleep(1000);
                System.out.println(Instant.now()+" Quickly Processed for record id "+record.value().getId()+" by thread "+Thread.currentThread().getName());
            }
            return true;
        }catch(InterruptedException ex){
            System.out.println(Instant.now()+" Error while processing record id "+ record.value().getId()+" by thread "+Thread.currentThread().getName());
            ex.printStackTrace();
            return false;
        }
    }


    private static void rejectDeserializationFailure(KafkaShareConsumer<String, EventMessage> consumer,
                                                     RecordDeserializationException exception) {
        String topic = exception.topicPartition().topic();
        int partition = exception.topicPartition().partition();
        long offset = exception.offset();
        System.err.println(Instant.now()+" Rejecting deserialization failure record id =" + topic
                + ", partition=" + partition + ", offset=" + offset
                + ": " + exception.getMessage());
        consumer.acknowledge(topic, partition, offset, AcknowledgeType.REJECT);
    }
}