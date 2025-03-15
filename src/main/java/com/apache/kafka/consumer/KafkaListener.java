package com.apache.kafka.consumer;

import com.apache.kafka.pojo.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Service;

@Service
public class KafkaListener {

    Logger log= LoggerFactory.getLogger(KafkaListener.class);

    @org.springframework.kafka.annotation.KafkaListener(
            topics = "beHappyPremaLatha-1",
            groupId = "happy-group",
    topicPartitions = {@TopicPartition(topic ="beHappyPremaLatha-1",partitions = {"1"})})
    public void consume(Customer customer){

        log.info("consumerGroup1 consume the message :{}",customer.toString());
    }
//    @org.springframework.kafka.annotation.KafkaListener(topics = "beHappyPremaLatha-1",groupId = "happy-group")
//    public void consume1(Customer message){
//        log.info("consumerGroup2 consume the message :{}",message.toString());
//    }
//    @org.springframework.kafka.annotation.KafkaListener(topics = "beHappyPremaLatha-1",groupId = "happy-group")
//    public void consume2(Customer message){
//        log.info("consumerGroup3 consume the message :{}",message.toString());
//    }
//    @org.springframework.kafka.annotation.KafkaListener(topics = "beHappyPremaLatha-1",groupId = "happy-group")
//    public void consume3(Customer message){
//        log.info("consumerGroup4 consume the message :{}",message.toString());
//    }
}
