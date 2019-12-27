package com.example.demo.hadoop;


import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class KafkaToHdfs {


//    private static String hdfsUri = "hdfs://47.98.225.98:9000";       //hadoop hdfs 连接地址
    private static String hdfsUri = "hdfs://39.100.62.56:9000";       //hadoop hdfs 连接地址
    private static String hdfsDir = "/test";                          //文件路径
    private static String hadoopUser = "hadoop";

    private static Configuration hdfsConf = null;
    private static FileSystem hadoopFS = null;

    public static void main(String[] args) {
        String aaa = "hello world hello lilei hello haimeimei hello hadoop hello girl hello girlaaaa啊啊啊啊啊啊啊啊啊啊啊啊";

        initHadoop();
//        wirteFile(aaa);
//        deleteFile("test.txt");
        upLoadFile("shangchuan.txt", null);
//        downLoadFile("2019-12-27.txt");
//        listFile();
    }

    public static void initHadoop() {

        log.info("开始启动服务...");

        hdfsConf = new Configuration();
        try {
            hdfsConf.set("fs.defaultFS", hdfsUri);
//            hdfsConf.set("dfs.replication", "1");
//            hdfsConf.set("dfs.support.append", "true");
            hdfsConf.set("dfs.client.use.datanode.hostname", "true");
//            hdfsConf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
//            hdfsConf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
//            hadoopFS = FileSystem.get(new URI(hdfsUri), hdfsConf, hadoopUser);
            hadoopFS = FileSystem.get(hdfsConf);
        } catch (Exception e) {
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
//            hadoopFS = FileSystem.get(new URI(hdfsUri), hdfsConf, hadoopUser);
            //如果hdfs的对应的目录不存在，则进行创建
            log.info("================= 验证目录是否存在:{}========================", hdfsDir);
            if (!hadoopFS.exists(new Path(hdfsDir))) {
                log.info("================= 没有此目录:{}========================", hdfsDir);
                hadoopFS.mkdirs(new Path(hdfsDir));
            }

            //文件名称
            String fileName = hdfsDir + "/" + (new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime())) + ".txt";
            Path path = new Path(fileName);
            log.info("================= 验证文件是否存在:{}========================", fileName);
            if (!hadoopFS.exists(path)) {
                log.info("================= 验证文件不存在========================");
                //创建文件
                FSDataOutputStream output = hadoopFS.create(path);
                output.close();
            }

            //文件追加内容
            log.info("=======================内容写入开始：{}=======================", fileContent);
            FSDataOutputStream out = hadoopFS.append(path);
            InputStream in = new ByteArrayInputStream(fileContent.getBytes("UTF-8"));
            IOUtils.copyBytes(in, out, 4096, true);
            log.info("=======================内容写入结束=======================");
        } catch (Exception e) {
            e.printStackTrace();
        } /*finally {
            try {
                hadoopFS.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }*/
    }

    /**
     * 文件上传
     *
     * @param fileName
     * @param in
     */
    public static void upLoadFile(String fileName, InputStream in) {

        try {
            in = new FileInputStream("E:\\shangchuan.txt");

//            hadoopFS = FileSystem.get(new URI(hdfsUri), hdfsConf, hadoopUser);
            Path path = new Path(hdfsDir + "/" + fileName);
            // true 如果文件存在则覆盖
            log.info("=======================内容上传开始=======================");
//            hadoopFS.copyFromLocalFile(new Path("/Users/yangpeng/Desktop/软件包/apache-hive-1.2.2-bin.tar.gz"),new Path("/"));
            FSDataOutputStream out = hadoopFS.create(path, true);
            IOUtils.copyBytes(in, out, 4096, true);
            log.info("=======================内容上传结束=======================");
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

    /**
     * 文件下载
     *
     * @param fileName
     */
    public static void downLoadFile(String fileName) {

        try {

            hadoopFS = FileSystem.get(new URI(hdfsUri), hdfsConf, hadoopUser);
            Path path = new Path(hdfsDir + "/" + fileName);
            // true 如果文件存在则覆盖
            log.info("======================文件下载=======================");
//            hadoopFS.copyToLocalFile(path, new Path("E:\\downLoadFile.txt"));
            if (hadoopFS.exists(path)){
                FSDataInputStream in = hadoopFS.open(path);
                OutputStream out = new FileOutputStream("E:\\downLoadFile.txt");
                IOUtils.copyBytes(in, out, 4096, true);
//                IOUtils.copyBytes(in, System.out, 4096, true);
            }
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

    /**
     * 文件删除
     */
    public static void deleteFile(String fileName) {

        try {
            hadoopFS = FileSystem.get(new URI(hdfsUri), hdfsConf, hadoopUser);
            //文件名称
            fileName = hdfsDir + "/" + fileName;
            Path path = new Path(fileName);
            log.info("================= 验证文件是否存在:{}========================", fileName);
            if (hadoopFS.exists(path)) {
                log.info("================= 文件删除========================");
                boolean delete = hadoopFS.delete(path, true);
                if (delete) {
                    log.info("=======================删除成功=======================");
                } else {
                    log.info("=======================删除失败=======================");
                }
            }
            log.info("=======================删除完成=======================");
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

    /**
     * 列表
     */
    public static void listFile() {

        try {
            hadoopFS = FileSystem.get(new URI(hdfsUri), hdfsConf, hadoopUser);
            Path path = new Path(hdfsDir);
            FileStatus[] fileStatuses = hadoopFS.listStatus(path);
            log.info("=======================文件列表=======================");
            for (FileStatus file : fileStatuses) {
                System.out.println("========文件列表========" + file);
            }
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