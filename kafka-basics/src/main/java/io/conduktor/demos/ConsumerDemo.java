package io.conduktor.demos;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static <ConsumerRecords> void main(String[] args) {
        log.info("[Kafka Consumer]");

        String groupId = "my-java-application";
        String topic = "demo_topic";

        // create Producer Properties
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // or connect to conduktor
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");

        // connect to Condutkor Playground
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "CREDENTIALS");

        // create consumers config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());

        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        // poll for data
        while(true){
            log.info("Polling");

            org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord <String, String> record: records) {
                log.info("Key:"+ record.key() + ", value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }
}
