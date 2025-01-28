package com.satya.service;

import com.satya.DTO.Customer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private RetryTemplate retryTemplate;

    private static final String topic = "k-topic1";
    private static final Logger log = Logger.getLogger(KafkaMessagePublisher.class.getName());

    public void publishToTopic(String message) {
        try {
            retryTemplate.execute(context -> {
                log.info("Attempt #" + (context.getRetryCount() + 1) + " to send message");

                try {
                    SendResult<String, Object> result = kafkaTemplate.send(topic, message).get();
                    log.info("Message sent = [" + message + "] with Offset: [" +
                            result.getRecordMetadata().offset() + "] in partition -> " +
                            result.getRecordMetadata().partition());
                    return result;
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof TimeoutException) {
                        log.severe("Kafka timeout: " + cause.getMessage());
                        throw new KafkaException("Kafka timeout", cause);
                    }
                    throw new KafkaException("Failed to send message", cause);
                }
            });
        } catch (KafkaException e) {
            log.severe("Final exception after retries: " + e.getMessage());
            throw new RuntimeException("Message could not be sent to Kafka: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public ResponseEntity<?> publishEventToTopic(Customer customer) {
        try {
            CompletableFuture<SendResult<String, Object>> result = kafkaTemplate.send(topic, customer);
            result.whenComplete((results, ex)-> {
                if (ex==null) {
                    log.info("Event sent : "+customer.toString());
                } else {
                    log.severe("There is some issue occured while sending events : "+ex.getMessage());
                }
            });
        } catch (Exception ex ) {
            log.severe("Exception! There is some issue occured while sending events : "+ex.getMessage());
        }

        return new ResponseEntity<>(HttpStatusCode.valueOf(200));
    }

    public String publishToSpecificPartition(Customer message, Integer partition) {
        try {
            CompletableFuture<SendResult<String, Object>> result = kafkaTemplate.send(topic, partition, null, message);
            result.whenComplete((results, ex)-> {
                if (ex==null) {
                    log.info("Event sent : "+message.toString()+" in Partition: "+partition);
                } else {
                    log.severe("There is some issue occured while sending events : "+ex.getMessage());
                }
            });
        } catch (Exception ex ) {
            log.severe("Exception! There is some issue occured while sending events : "+ex.getMessage());
        }

        return "Success";
    }

}
