package com.example.demo.kafka.controller;

import com.example.demo.kafka.manager.KafkaProducer;
import com.example.demo.kafka.manager.OrderInfoGenerator;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;

@Controller
@RequestMapping("kafka")
public class KafkaRequestMessageController {

    @Resource
    KafkaProducer kafkaProducer;

    @GetMapping("index")
    public String test(){
        return "/index.html";
    }

    /**
     * 接收提交过来的信息通过kafka保存在hdfs
     * @param kafkaContent
     */
    @ResponseBody
    @RequestMapping("req")
    public void reqMessage(String kafkaContent){
        kafkaProducer.sendMsg(kafkaContent);

    }

    /**
     * 自动生成订单信息发送到kafka保存在hdfs
     * @param num  订单信息条数
     */
    @ResponseBody
    @RequestMapping("generator")
    public void generator(Integer num){
        OrderInfoGenerator generator = new OrderInfoGenerator();
        List<String> generate = generator.generate(num);
        for (String message : generate) {
            kafkaProducer.sendMsg(message);
        }
    }

}
