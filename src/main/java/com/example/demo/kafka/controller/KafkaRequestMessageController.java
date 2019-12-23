package com.example.demo.kafka.controller;

import com.example.demo.kafka.manager.KafkaProducer;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.annotation.Resource;

@Controller
@RequestMapping("kafka")
public class KafkaRequestMessageController {

    @Resource
    KafkaProducer kafkaProducer;

    @GetMapping("req/{message}")
    public void reqMessage(@PathVariable("message") String message){
        kafkaProducer.sendMsg(message);

    }

}
