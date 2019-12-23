package com.example.demo.kafka.manager;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * @Author lyh
 * @Date 2019/12/21
 * @Description //消费者
 */
@Slf4j
@Component
public class KafkaConsumer {

    @KafkaListener(topics = {"testTopic"}, groupId = "receiver")
    public void consumerMsg(ConsumerRecord<?, ?> record) {
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());


        //https://blog.csdn.net/u013385018/article/details/80689546

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(kafkaTopic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        if (kafkaMessage.isPresent()) {
            Object message = kafkaMessage.get();
            log.info("消费---------------- record =" + record);
            log.info("消费---------------- message =" + message);
        }
    }


    /*//指定监听的topic，当前消费者组id
    @KafkaListener(topics = {"test"}, groupId = "receiver")
    public void registryReceiver(ConsumerRecord<Integer, String> integerStringConsumerRecords) {
        log.info(integerStringConsumerRecords.value());
    }


    // 简单消费者
    @KafkaListener(groupId = "simpleGroup", topics = {"test"})
    public void consumer1_1(ConsumerRecord<String, Object> record, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, Consumer consumer, Acknowledgment ack) {
        log.info("单独消费者消费消息,topic= {} ,content = {}",topic,record.value());
        log.info("consumer content = {}",consumer);
        ack.acknowledge();
    }*/
}