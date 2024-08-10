package com.vamshilearning.controller;

import com.vamshilearning.dto.Customer;
import com.vamshilearning.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer-app")
public class EventController {

    @Autowired
    private KafkaMessagePublisher kafkaMessagePublisher;

    @GetMapping("/publish/{message}")
    public ResponseEntity<?> publishEvent(@PathVariable String message) {

        try {
            for( int i = 0; i < 10000; i++ )
            { kafkaMessagePublisher.sendMessageToTopic(message + ":" + i);}

            return ResponseEntity.ok("message is successful");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }


    @PostMapping ("/publishMessage")
    public void sendEvent(@RequestBody Customer message) {

        kafkaMessagePublisher.eventsToTopic(message);
    }

}
