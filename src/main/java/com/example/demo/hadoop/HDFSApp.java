package com.example.demo.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * @Author lyh
 * @Date 2019/12/22
 * @Description //TODO
 */
@Slf4j
public class HDFSApp {

    public static final String HDFS_PATH = "http://39.100.62.56:8020";
    private static String hdfsUri = "hdfs://39.100.62.56:9000";
    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsUri);
        configuration.set("dfs.support.append", "true");
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");

        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "root");
        log.info("HDFSApp.setUp");
    }

    /**
     * 创建文件夹
     *
     * @throws Exception
     */
    public void mkdir(String hdfsDir) throws Exception {
        //如果hdfs的对应的目录不存在，则进行创建
        if (!fileSystem.exists(new Path("/" + hdfsDir))) {
            fileSystem.mkdirs(new Path("/" + hdfsDir));
        }
    }

    /**
     * 新建文件
     *
     * @throws Exception
     */
    public void create() throws Exception {
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsdat/test/a.txt"));
        output.write("hello baby".getBytes());
        output.flush();
        output.close();
    }

    /**
     * 查看hdfs文件的内容
     *
     * @throws Exception
     */
    public void cat() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/hdfsdat/test/a.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
        in.close();
    }

    /**
     * 重命名
     *
     * @throws Exception
     */
    public void rename() throws Exception {
        Path oldPath = new Path("/hdfsdat/test/a.txt");
        Path newPath = new Path("/hdfsdat/test/b.txt");
        fileSystem.rename(oldPath, newPath);
    }

    /**
     * 上传文件到hdfs
     *
     * @throws Exception
     */
    public void copyFromLocalFile() throws Exception {
        Path localPath = new Path("D:/data/h.txt");
        Path hdfsPath = new Path("/hdfsdat/test");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
    }

    /**
     * 上传大文件到hdfs，带进度条
     *
     * @throws Exception
     */
    public void copyFromLocalFileWithProgress() throws Exception {
        InputStream in = new BufferedInputStream(new FileInputStream(
                new File("D:/downloads/spark-2.1.0-bin-2.6.0-cdh5.7.0.tgz")
        ));
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsdat/test/spark-2.1.0-bin-2.6.0-cdh5.7.0.tgz"),
                new Progressable() {
                    @Override
                    public void progress() {
                        System.out.print("*");//进度提醒
                    }
                });
        IOUtils.copyBytes(in, output, 4096);
    }

    /**
     * 下载文件到本地
     *
     * @throws Exception
     */
    public void copyTolocalFile() throws Exception {
        Path localPath = new Path("D:/data/h.txt");
        Path hdfsPath = new Path("/hdfsdat/test/h.txt");
        fileSystem.copyToLocalFile(false, hdfsPath, localPath, true);
    }

    public void listFiles() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfsdat/test"));
        for (FileStatus fileStatus : fileStatuses) {
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            //几个副本
            short replication = fileStatus.getReplication();
            //文件的大小
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);
        }
    }

    public void delete() throws Exception {
        fileSystem.delete(new Path("/hdfsdat/test/a.txt"), false);//第二个参数指是否递归删除
    }

    @After
    public void tearDown() throws Exception {
        configuration = null;
        fileSystem = null;
        System.out.println("HDFSApp.tearDown");
    }
}