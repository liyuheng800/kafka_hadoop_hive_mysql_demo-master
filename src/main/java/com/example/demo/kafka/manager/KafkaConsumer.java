package com.example.demo.kafka.manager;

import com.example.demo.hadoop.HDFSApp;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Optional;

/**
 * @Author lyh
 * @Date 2019/12/21
 * @Description //消费者
 */
@Slf4j
@Component
public class KafkaConsumer {

    @Resource
    HDFSApp hdfsApp;

    @KafkaListener(topics = {"testTopic"}, groupId = "receiver")
    public void consumerMsg(ConsumerRecord<?, ?> record) {
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        if (kafkaMessage.isPresent()) {
            Object message = kafkaMessage.get();
            log.info("消费---------------- record =" + record);
            log.info("消费---------------- message =" + message);
            try {
                hdfsApp.HdfsWrite(message);
            } catch (Exception e) {
                log.error("数据写入HDFS异常");
                e.printStackTrace();
            }
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