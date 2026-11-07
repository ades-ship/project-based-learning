package com.example.tech.kafka.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class kafkaConfig {
    private static final Properties properties = new Properties();

    static {
        try (InputStream input = kafkaConfig.class.getClassLoader().getResourceAsStream("kafka.properties")) {
            if (input == null) {
                throw new RuntimeException("Sorry, unable to find kafka.properties in classpath");
            }
            properties.load(input);
        } catch (IOException ex) {
            throw new RuntimeException("Error loading configuration file", ex);
        }
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    public static String getBootstrapServers() {
        return getProperty("kafka.bootstrap.servers");
    }

    public static String getTopicName() {
        return getProperty("kafka.topic.name");
    }

    public static String getConsumerGroupId() {
        return getProperty("kafka.consumer.group.id");
    }

    public static String getAutoOffsetReset() {
        return getProperty("kafka.consumer.auto.offset.reset");
    }
}
