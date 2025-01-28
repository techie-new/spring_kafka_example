package com.satya.controller;

import com.satya.DTO.Customer;
import com.satya.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping("/kafka")
public class KafkaController {
    @Autowired
    private KafkaMessagePublisher publisher;

    @PostMapping("/publish")
    public ResponseEntity<?> publish(@RequestBody String message){
        try {
            for(int i =0;i<1000000;i++) {
                publisher.publishToTopic(message);
            }
            return ResponseEntity.ok("Message sent successfully");
        } catch(Exception ex) {
            return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).build();
        }
    }

    @PostMapping("/publishEvent")
    public ResponseEntity<?> publishEvents(@RequestBody Customer customer){
        try {
            for(int i =0;i<3;i++) {
                publisher.publishEventToTopic(customer);
            }
            return ResponseEntity.ok("Message sent successfully");
        } catch(Exception ex) {
            return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).build();
        }
    }

    @PostMapping("/publish/{partition}")
    public ResponseEntity<?> publish(@RequestBody Customer message, @PathVariable int partition){
        try {
            for(int i =0;i<100;i++) {
                publisher.publishToSpecificPartition(message,partition);
            }
            return ResponseEntity.ok("Message sent successfully in partition : "+partition);
        } catch(Exception ex) {
            return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).build();
        }
    }


    @GetMapping("/test")
    public ResponseEntity<String> test() {
        return ResponseEntity.ok("Test endpoint working!");
    }

}
