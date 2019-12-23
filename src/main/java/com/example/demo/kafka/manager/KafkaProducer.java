package com.example.demo.kafka.manager;

import com.example.demo.hadoop.HDFSTools;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import javax.annotation.Resource;


/**
 * @Author lyh
 * @Date 2019/12/21
 * @Description //消息生产者
 */
@Slf4j
@Component
public class KafkaProducer {

    @Resource
    private KafkaTemplate kafkaTemplate;

    public void sendMsg(Object msgObj) {
        //发送消息
//        kafkaTemplate.send("test", 0, 12, "1222");
//        log.info("kafka---------------- 发送消息:{}", msgObj);
        ListenableFuture testTopic = kafkaTemplate.send("testTopic", msgObj);
    }

}