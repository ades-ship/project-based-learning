package com.example.tech;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.example.tech.service.KafkaConsumerService;
import com.example.tech.service.kafkaProducerService;
@SpringBootApplication
public class TechApplication {

	 private static final String BROKERS = "localhost:9092";
    private static final String MY_TOPIC = "test-topic";

    public static void main(String[] args) throws InterruptedException {
        // 1. Initialize and start the segregated consumer in a background thread
        KafkaConsumerService consumerService = new KafkaConsumerService();
        Thread consumerThread = new Thread(consumerService);
        consumerThread.start();

        // Let the listener stand up completely
        Thread.sleep(1500);

        // 2. Instantiate and use the producer safely using try-with-resources
        System.out.println("[Main] Triggering segregated Producer service...");
        try (kafkaProducerService producerService = new kafkaProducerService()) {
            
            producerService.sendMessage(MY_TOPIC, "id-101", "Payload Alpha");
            Thread.sleep(500);
            
            producerService.sendMessage(MY_TOPIC, "id-102", "Payload Beta");
            Thread.sleep(500);
        }

        // Give the consumer thread time to print out the final incoming records
        Thread.sleep(2000);

        // 3. Gracefully shut down the consumer thread before application exit
        System.out.println("[Main] Stopping active background threads...");
        consumerService.stop();
    }

}
