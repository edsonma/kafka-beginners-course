package io.conduktor.demos;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());
    public static void main(String[] args) {
        log.info("[Kafka Producer]");

        // create Producer Properties
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // or connect to conduktor
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");

        // connect to Conduktor Playground
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "CREDENTIALS");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j=0; j<2; j++) {
            for(int i=0; i<10; i++) {
                String topic = "demo_topic";
                String key = "id_" + i;
                String value = "hello kafka " + i;

                // create the Producer record
                ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic, key, value);

                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception){
                        if (exception == null) {
                            // the record was successfully sent
                            log.info(
                                "[Timestamp: " + metadata.timestamp() + "] " +
                                "Key: " + key + " | " +
                                "Partition: " + metadata.partition() + " | " +
                                "Offset: " + metadata.offset()
                            );

                        } else {
                            log.error("Error while producing" + exception);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // tell producer to send all data and block until done - sync
        producer.flush();

        // close the producer
        producer.close();
    }
}
