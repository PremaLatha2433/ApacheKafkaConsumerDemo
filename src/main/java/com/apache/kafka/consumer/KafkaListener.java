package com.apache.kafka.consumer;

import com.apache.kafka.pojo.Customer;
import com.apache.kafka.pojo.User;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.headers.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Stream;

@Service
public class KafkaListener {

    Logger log= LoggerFactory.getLogger(KafkaListener.class);

    @org.springframework.kafka.annotation.KafkaListener(
            topics = "beHappyPremaLatha-1",
            groupId = "happy-group",
    topicPartitions = {@TopicPartition(topic ="beHappyPremaLatha-1",partitions = {"1"})})
    public void consume(Customer message){
        log.info("consumerGroup1 consume the message :{}",message.toString());
    }
    @org.springframework.kafka.annotation.KafkaListener(topics = "beHappyPremaLatha-1",groupId = "happy-group")
    public void consume1(Customer message){
        log.info("consumerGroup2 consume the message :{}",message.toString());
    }

    @RetryableTopic(attempts = "4"  //attempts 3
            //exclude = {NullPointerException.class}, // no retry for particular exception
            //backoff = @Backoff(delay = 3000,multiplier = 1.5,maxDelay = 15000) //delay for every retry
             )
    @org.springframework.kafka.annotation.KafkaListener(
            topics="beHappyPremaLatha-2",
            groupId = "happy-group")
   public void consumeUser(User user,
       @org.springframework.messaging.handler.annotation.Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
       @org.springframework.messaging.handler.annotation.Header(KafkaHeaders.OFFSET) int offset)  {
        try {
            log.info("Received:{} from {} offset {}", new ObjectMapper().writeValueAsString(user), topic, offset);
            List<String> restrictedIpList = Stream.of("32.241.234", "32.241.236", "32.241.237", "32.241.239").toList();
            if (restrictedIpList.contains(user.getIpAddress())) {
                throw new RuntimeException("Invalid Ip Address Received...");
            }
        }catch (JsonProcessingException e){
            log.info("Unable to send message:"+e.getMessage());
            e.printStackTrace();
        }
   }

   @DltHandler
   public void listenDLT(User user,
                         @org.springframework.messaging.handler.annotation.Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                              @org.springframework.messaging.handler.annotation.Header(KafkaHeaders.OFFSET) int offset){

       log.info("DLT Received:{} from {} offset {}", user.getName(), topic, offset);

   }
}
