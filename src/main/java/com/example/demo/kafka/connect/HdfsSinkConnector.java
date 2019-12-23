package com.example.demo.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author lyh
 * @Date 2019/12/22
 * @ClassName HdfsSinkConnector
 * @Description //kafka coneeector sink 配置
 */

public class HdfsSinkConnector extends SinkConnector {

    //这两项为配置hdfs的urlh和路径的配置项，需要在connector1.properties中指定
    public static final String HDFS_URL = "hdfs.url";
    public static final String HDFS_PATH = "hdfs.path";
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(HDFS_URL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "hdfs url")
            .define(HDFS_PATH, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "hdfs path");
    private String hdfsUrl;
    private String hdfsPath;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    //start方法会再初始的时候执行一次，这里主要用于配置
    @Override
    public void start(Map<String, String> props) {
        hdfsUrl = props.get(HDFS_URL);
        hdfsPath = props.get(HDFS_PATH);
    }

    //这里指定了Task的类
    @Override
    public Class<? extends Task> taskClass() {
        return HdfsSinkTask.class;
    }

    //用于配置Task的config，这些都是会在Task中用到
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            if (hdfsUrl != null)
                config.put(HDFS_URL, hdfsUrl);
            if (hdfsPath != null)
                config.put(HDFS_PATH, hdfsPath);
            configs.add(config);
        }
        return configs;
    }

    //关闭时的操作，一般是关闭资源。
    @Override
    public void stop() {
        // Nothing to do since FileStreamSinkConnector has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

}