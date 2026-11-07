package com.example.tech.service;

import com.example.tech.kafka.config.kafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class kafkaProducerService implements AutoCloseable {

    private final KafkaProducer<String, String> producer;

    public kafkaProducerService() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
    }

    public void sendMessage(String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.printf("[Producer] Sent: Key=%s, Value=%s to Partition=%d, Offset=%d%n",
                        key, value, metadata.partition(), metadata.offset());
            } else {
                System.err.println("[Producer] Error sending message: " + exception.getMessage());
            }
        });
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
            System.out.println("[Producer] Connection closed.");
        }
    }
}
