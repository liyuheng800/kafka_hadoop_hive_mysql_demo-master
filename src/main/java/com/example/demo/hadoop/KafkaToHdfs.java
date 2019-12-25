package com.example.demo.hadoop;


import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class KafkaToHdfs {


    private static String hdfsUri = "hdfs://39.100.62.56:9000";       //hadoop hdfs 连接地址
    private static String hdfsDir = "/test";                          //文件路径
    private static String hadoopUser = "hadoop";

    private static Configuration hdfsConf = null;
    private static FileSystem hadoopFS = null;

    public static void main(String[] args) {
        String aaa = "hello world hello lilei hello haimeimei hello hadoop hello girl hello girl";

        initHadoop();
        wirteFile(aaa);
    }

    public static void initHadoop() {

        log.info("开始启动服务...");

        hdfsConf = new Configuration();
        try {
            hdfsConf.set("fs.defaultFS", hdfsUri);
            hdfsConf.set("dfs.support.append", "true");
            hdfsConf.set("dfs.client.use.datanode.hostname", "true");
            hdfsConf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
            hdfsConf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        } catch (Exception e) {
            e.printStackTrace();
        }

        //创建好相应的目录
        try {
            hadoopFS = FileSystem.get(new URI(hdfsUri), hdfsConf, hadoopUser);
            //如果hdfs的对应的目录不存在，则进行创建
            log.info("================= 验证目录是否存在:{}========================", hdfsDir);
            if (!hadoopFS.exists(new Path(hdfsDir))) {
                log.info("================= 没有此目录:{}========================", hdfsDir);
                hadoopFS.mkdirs(new Path(hdfsDir));
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        log.info("服务启动完毕，监听执行中");
    }

    /**
     * 创建文件/数据写入
     *
     * @param fileContent 文件内容
     */
    public static void wirteFile(String fileContent) {

        try {
            //文件名称
            String fileName = hdfsDir + "/" + (new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime())) + ".txt";
            Path dst = new Path(fileName);
            log.info("================= 验证文件是否存在:{}========================", fileName);
            if (!hadoopFS.exists(dst)) {
                log.info("================= 验证文件不存在========================");
                //创建文件
                FSDataOutputStream output = hadoopFS.create(dst);
                output.close();
            }
            //文件追加内容
            log.info("=======================内容写入开始=======================");
            InputStream in = new ByteArrayInputStream(fileContent.getBytes("UTF-8"));
            FSDataOutputStream out = hadoopFS.append(dst);
            out.write(fileContent.getBytes("UTF-8"));
            out.flush();
            out.close();
            log.info("=======================内容写入结束{}=======================", fileContent);
//            IOUtils.copyBytes(in, out, 4096, true);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                hadoopFS.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}