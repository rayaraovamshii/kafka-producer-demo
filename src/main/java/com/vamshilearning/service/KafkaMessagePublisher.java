package com.vamshilearning.service;

import com.vamshilearning.dto.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> template;

    public void sendMessageToTopic(String message) {
//        CompletableFuture<SendResult<String, Object>> future =
//                template.send("v-topic", 3, null, message);
//        future.whenComplete((result, ex) -> {
//            if (ex != null) {
//                System.out.println("sent Message" + message + " with off set : "
//                        + result.getRecordMetadata().offset() );
//            } else {
//                System.out.println("exception" + ex.getMessage());
//            }
//        });


                template.send("v-topic", 4, null, "HI-0");

                template.send("v-topic", 0, null, "HI  w");

                template.send("v-topic", 1, null, "HI-k");

                template.send("v-topic", 2, null, "HI-v");
                template.send("v-topic", 2, null, "HI-N");

    }

    public void eventsToTopic( Customer message) {
        try {
            CompletableFuture<SendResult<String, Object>> future =
                    template.send("topicThroughBeanCreation2", message);
            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    System.out.println("sent Message" + message.toString() + " with off set : " +
                            result.getRecordMetadata().offset());
                } else {
                    System.out.println("exception" + ex.getMessage());
                }
            });
        }
        catch (Exception e) {
            System.out.println( "ERROR" + e.getMessage());
        }
    }

}
