package com.satya.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic kafkaTopic() {
        return new NewTopic("k-topic6", 4, (short) 1);
    }
}
