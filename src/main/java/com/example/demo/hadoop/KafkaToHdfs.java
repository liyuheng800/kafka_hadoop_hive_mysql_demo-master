package com.example.demo.hadoop;


import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

@Slf4j
@Component
public class KafkaToHdfs {


    //    private static String hdfsUri = "hdfs://47.98.225.98:9000";       //hadoop hdfs 连接地址
    private static String hdfsUri = "hdfs://39.100.62.56:9000";       //hadoop hdfs 连接地址
    private static String hdfsDir = "/test";                          //文件路径
    private static String hadoopUser = "hadoop";

    private static Configuration hdfsConf = null;
    private static FileSystem hadoopFS = null;

//    public static void main(String[] args) {
//        String aaa = "hello world hello lilei hello haimeimei hello hadoop hello girl hello girlaaaa啊啊啊啊啊啊啊啊啊啊啊啊";
//
//        initHadoop();
//        wirteFile(aaa);
//        deleteFile("test.txt");
//        upLoadFile("shangchuan.txt", null);
//        downLoadFile("shangchuan.txt");
//        listFile();
//    }

    public void initHadoop() {

        log.info("开始启动服务...");

        hdfsConf = new Configuration();
        try {
//            hdfsConf.set("dfs.replication", "1");
//            hdfsConf.set("dfs.support.append", "true");
//            hadoopFS = FileSystem.get(new URI(hdfsUri), hdfsConf, hadoopUser);

            hdfsConf.set("fs.defaultFS", hdfsUri);
            hdfsConf.set("dfs.client.use.datanode.hostname", "true");
            hdfsConf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
            hdfsConf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
            hadoopFS = FileSystem.get(hdfsConf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.info("服务启动完毕，监听执行中");
    }

    /**
     * 创建文件/数据写入
     *
     * @param fileName    文件名称
     * @param fileContent 文件内容
     */
    public String wirteFile(String fileName, String fileContent) {
        initHadoop();
        try {
            //如果hdfs的对应的目录不存在，则进行创建
            log.info("================= 验证目录是否存在:{}========================", hdfsDir);
            if (!hadoopFS.exists(new Path(hdfsDir))) {
                log.info("================= 没有此目录:{}========================", hdfsDir);
                hadoopFS.mkdirs(new Path(hdfsDir));
            }

            //文件名称
            fileName = StringUtils.isEmpty(fileName) ? (new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime())) + ".txt" : fileName;
            Path path = new Path(hdfsDir + "/" + fileName);
            log.info("================= 验证文件是否存在:{}========================", fileName);
            if (!hadoopFS.exists(path)) {
                log.info("================= 文件不存在========================");
                //创建文件
                FSDataOutputStream output = hadoopFS.create(path);
                output.close();
            } else {
                log.info("================= 文件存在========================");
            }

            //文件追加内容
            log.info("=======================内容写入开始：{}=======================", fileContent);
            FSDataOutputStream out = hadoopFS.append(path);
            InputStream in = new ByteArrayInputStream(fileContent.getBytes("UTF-8"));
            IOUtils.copyBytes(in, out, 4096, true);
            log.info("=======================内容写入结束=======================");
        } catch (Exception e) {
            e.printStackTrace();
            return "文件写入失败";
        } finally {
            try {
                hadoopFS.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return "文件写入成功";
    }

    /**
     * 文件上传
     *
     * @param file
     */
    public String upLoadFile(@RequestParam("file") MultipartFile file) {
        initHadoop();
        try {
            String fileName = file.getOriginalFilename();
            Path path = new Path(hdfsDir + "/" + fileName);
            // true 如果文件存在则覆盖
            log.info("=======================内容上传开始=======================");
            FSDataOutputStream out = hadoopFS.create(path, true);
            IOUtils.copyBytes(file.getInputStream(), out, 4096, true);
            log.info("=======================内容上传结束=======================");
        } catch (Exception e) {
            e.printStackTrace();
            return "文件上传失败";
        } finally {
            try {
                hadoopFS.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return "文件上传成功";
    }

    /**
     * 文件删除
     */
    public String deleteFile(String fileName) {
        initHadoop();
        try {
            //文件名称
            fileName = hdfsDir + "/" + fileName;
            Path path = new Path(fileName);
            log.info("================= 验证文件是否存在:{}========================", fileName);
            if (hadoopFS.exists(path)) {
                log.info("================= 文件删除开始========================");
                boolean delete = hadoopFS.delete(path, true);
                if (delete) {
                    log.info("=======================删除成功=======================");
                    return "文件删除成功";
                }
            }
            log.info("=======================文件不存在=======================");
            return "文件不存在";
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                hadoopFS.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        log.info("=======================删除失败=======================");
        return "文件删除失败";
    }

    /**
     * 列表
     */
    public List<HdfsEntity> listFile() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        initHadoop();
        try {
            Path path = new Path(hdfsDir);
            FileStatus[] fileStatuses = hadoopFS.listStatus(path);
            log.info("=======================文件列表=======================");
            List<HdfsEntity> list = new ArrayList<>();
            for (FileStatus file : fileStatuses) {
//                System.out.println("========文件列表========" + file);
                Path pa = file.getPath();
                String[] split = pa.toString().split("/");
                String feilName = split[split.length - 1];

                list.add(new HdfsEntity(feilName, file.getLen(), format.format(file.getModificationTime()), file.getReplication()));
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
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
     */
    public void openFile(HttpServletResponse response, String fileName) {

        log.info("=======================文件查看=======================");

        initHadoop();
        try {
            Path path = new Path(hdfsDir + "/" + fileName);
            if (hadoopFS.exists(path)) {

                FSDataInputStream in = hadoopFS.open(path);

                OutputStream out = response.getOutputStream();
                int len = -1;
                byte buffer[] = new byte[1024];
                while ((len = in.read(buffer)) != -1) {
                    out.write(buffer, 0, buffer.length);
                }


                response.setHeader("Content-disposition", "attachment;filename=" + fileName);  //客户端得到的文件名
                response.setContentType("application/x-download");//设置为下载application/x-download
                response.setContentType("text/html; charset=UTF-8");
                response.setHeader("Cache-Control", "no-cache");
                response.setHeader("Cache-Control", "no-store");
                response.setDateHeader("Expires", 0);
                response.setHeader("Pragma", "no-cache");

                out.flush();
                out.close();
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