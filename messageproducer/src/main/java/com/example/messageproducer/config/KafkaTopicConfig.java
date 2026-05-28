package com.example.messageproducer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;

@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.topic.name}")
    private String topic;

    @Bean
    public NewTopic topic() {
        return new NewTopic(topic, 1, (short) 1);
    }
}