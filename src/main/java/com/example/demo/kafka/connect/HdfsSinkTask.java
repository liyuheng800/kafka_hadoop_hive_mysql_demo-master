package com.example.demo.kafka.connect;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * @Author lyh
 * @Date 2019/12/22
 * @ClassName HdfsSinkTask
 * @Description //Task 配置  取出kafka数据放到目标容器(hdfs,msql,es,redis...)
 */
@Slf4j
public class HdfsSinkTask extends SinkTask {

    private String filename;

    public static String hdfsUrl;
    public static String hdfsPath;
    private Configuration conf;
    private FSDataOutputStream os;
    private FileSystem hdfs;


    public HdfsSinkTask() {

    }

    @Override
    public String version() {
        return new HdfsSinkConnector().version();
    }

    //Task开始会执行的代码，可能有多个Task，所以每个Task都会执行一次
    @Override
    public void start(Map<String, String> props) {
        hdfsUrl = props.get(HdfsSinkConnector.HDFS_URL);
        hdfsPath = props.get(HdfsSinkConnector.HDFS_PATH);

        log.info("----------------------------------- start--------------------------------");

        conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrl);
        //这两个是与hdfs append相关的设置
        conf.setBoolean("dfs.support.append", true);
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        try {
            hdfs = FileSystem.get(conf);
//            connector.hdfs = new Path(HDFSPATH).getFileSystem(conf);
            os = hdfs.append(new Path(hdfsPath));
        } catch (IOException e) {
            log.error(e.toString());
        }

    }

    //核心操作，put就是将数据从kafka中取出，存放到其他地方去
    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord record : sinkRecords) {
            log.trace("Writing line to {}: {}", logFilename(), record.value());
            try {
                log.info("write info------------------------" + record.value().toString() + "-----------------");
                os.write((record.value().toString()).getBytes("UTF-8"));
                os.hsync();
            } catch (Exception e) {
                log.error(e.toString());
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        try {
            os.hsync();
        } catch (Exception e) {
            log.error(e.toString());
        }

    }

    //同样是结束时候所执行的代码，这里用于关闭hdfs资源
    @Override
    public void stop() {
        try {
            os.close();
        } catch (IOException e) {
            log.error(e.toString());
        }
    }

    private String logFilename() {
        return filename == null ? "stdout" : filename;
    }


}