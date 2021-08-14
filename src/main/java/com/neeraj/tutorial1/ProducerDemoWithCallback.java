package com.neeraj.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {

        // Create a logger for ProducerDemoWithCallback
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServers = "localhost:9092";

        // Step1: Create producer properties
        Properties properties = new Properties();

        /* Old way of setting properties. Prone to typo.
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
         */

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Step2: Create producer
        KafkaProducer<String, String > producer = new KafkaProducer<>(properties);

        for(int i = 1; i <= 10; i++) {

            // Step3.0: Create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("java_topic", "hello world" + i);

            // Step3.1: Send data
            producer.send(record, new Callback() {
                // executes every time a record is successfully sent or exception is thrown
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null) {
                        // the record was successfully sent
                        logger.info("\nReceived new metadata =>" +
                                " Topic: " + recordMetadata.topic() +
                                " Partition: " + recordMetadata.partition() +
                                " Offset: " + recordMetadata.offset() +
                                " Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
