package com.example.demo.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.*;

public class KafkaToHdfs extends Thread {
    private static String kafkaHost = null;
    private static String kafkaGroup = null;
    private static String kafkaTopic = null;
    private static String hdfsUri = null;
    private static String hdfsDir = null;
    private static String hadoopUser = null;
    private static Boolean isDebug = false;
 
//    private ConsumerConnector consumer = null;
 
    private static Configuration hdfsConf = null;
    private static FileSystem hadoopFS = null;
 
    public static void main(String[] args) {
//        if (args.length < 6) {
//            useage();
//            System.exit(0);
//        }
//        Map<String, String> user = new HashMap<String, String>();
//        user = System.getenv();
//        user.put("HADOOP_USER_NAME","hadoop");
//        if (user.get("HADOOP_USER_NAME") == null) {
//            System.out.println("请设定hadoop的启动的用户名，环境变量名称：HADOOP_USER_NAME，对应的值是hadoop的启动的用户名");
//            System.exit(0);
//        } else {
//            hadoopUser = user.get("HADOOP_USER_NAME");
//        }
        hadoopUser = "hadoop";
        init(args);
 
        System.out.println("开始启动服务...");
 
        hdfsConf = new Configuration();
        try {
            hdfsConf.set("fs.defaultFS", hdfsUri);
            hdfsConf.set("dfs.support.append", "true");
            hdfsConf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
            hdfsConf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        } catch (Exception e) {
            System.out.println(e);
        }
 
        //创建好相应的目录
        try {
            hadoopFS = FileSystem.get(hdfsConf);
            //如果hdfs的对应的目录不存在，则进行创建
            if (!hadoopFS.exists(new Path("/" + hdfsDir))) {
                hadoopFS.mkdirs(new Path("/" + hdfsDir));
            }
            hadoopFS.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
 
        KafkaToHdfs selfObj = new KafkaToHdfs();
        selfObj.start();
 
        System.out.println("服务启动完毕，监听执行中");
    }
 
    public void run() {
//        Properties props = new Properties();
//        props.put("zookeeper.connect", kafkaHost);
//        props.put("group.id", kafkaGroup);
//
//        props.put("zookeeper.session.timeout.ms", "10000");
//        props.put("zookeeper.sync.time.ms", "200");
//        props.put("auto.commit.interval.ms", "1000");
//        props.put("auto.offset.reset", "smallest");
//        props.put("format", "binary");
//        props.put("auto.commit.enable", "true");
//        props.put("serializer.class", "kafka.serializer.StringEncoder");
//
//        ConsumerConfig consumerConfig = new ConsumerConfig(props);
//        this.consumer = Consumer.createJavaConsumerConnector(consumerConfig);
//
//        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
//        topicCountMap.put(kafkaTopic, new Integer(1));
//        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
//        KafkaStream<byte[], byte[]> stream = consumerMap.get(kafkaTopic).get(0);
//        ConsumerIterator<byte[], byte[]> it = stream.iterator();
//
//        while (it.hasNext()) {
//            String tmp = new String(it.next().message());
//            String fileContent = null;
//
//            if (!tmp.endsWith("\n"))
//                fileContent = new String(tmp + "\n");
//            else
//                fileContent = tmp;
//
//            debug("receive: " + fileContent);
//
//            try {
//                hadoopFS = FileSystem.get(hdfsConf);
//
//                String fileName = "/" + hdfsDir + "/" +
//                        (new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime())) + ".txt";
//
//                Path dst = new Path(fileName);
//
//                if (!hadoopFS.exists(dst)) {
//                    FSDataOutputStream output = hadoopFS.create(dst);
//                    output.close();
//                }
//
//                InputStream in = new ByteArrayInputStream(fileContent.getBytes("UTF-8"));
//                OutputStream out = hadoopFS.append(dst);
//                IOUtils.copyBytes(in, out, 4096, true);
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            } finally {
//                try {
//                    hadoopFS.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        consumer.shutdown();
    }
 
    private static void init(String[] args) {
        kafkaHost = "10.175.118.105:2182";
        kafkaGroup = "test-consumer-group";
        kafkaTopic = "test";
        hdfsUri = "hdfs://39.100.62.56:9000";
        hdfsDir = "test";
        if (args.length > 5) {
            if (args[5].equals("true")) {
                isDebug = true;
            }
        }
 
        debug("初始化服务参数完毕,参数信息如下");
        debug("KAFKA_HOST: " + kafkaHost);
        debug("KAFKA_GROUP: " + kafkaGroup);
        debug("KAFKA_TOPIC: " + kafkaTopic);
        debug("HDFS_URI: " + hdfsUri);
        debug("HDFS_DIRECTORY: " + hdfsDir);
        debug("HADOOP_USER: " + hadoopUser);
        debug("IS_DEBUG: " + isDebug);
    }
 
    private static void debug(String str) {
        if (isDebug) {
            System.out.println(str);
        }
    }
 
    private static void useage() {
        System.out.println("* kafka写入到hdfs的Java工具使用说明 ");
        System.out.println("# java -cp kafkatohdfs.jar KafkaToHdfs KAFKA_HOST KAFKA_GROUP KAFKA_TOPIC HDFS_URI HDFS_DIRECTORY IS_DEBUG");
        System.out.println("*  参数说明:");
        System.out.println("*   KAFKA_HOST      : 代表kafka的主机名或IP:port，例如：namenode:2181,datanode1:2181,datanode2:2181");
        System.out.println("*   KAFKA_GROUP     : 代表kafka的组，例如：test-consumer-group");
        System.out.println("*   KAFKA_TOPIC     : 代表kafka的topic名称 ，例如：usertags");
        System.out.println("*   HDFS_URI        : 代表hdfs链接uri ，例如：hdfs://namenode:9000");
        System.out.println("*   HDFS_DIRECTORY  : 代表hdfs目录名称 ，例如：usertags");
        System.out.println("*  可选参数:");
        System.out.println("*   IS_DEBUG        : 代表是否开启调试模式，true是，false否，默认为false");
    }
}