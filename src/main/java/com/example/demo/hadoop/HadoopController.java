package com.example.demo.hadoop;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.util.List;

@Controller
@RequestMapping("hadoop")
public class HadoopController {

    @Resource
    KafkaToHdfs kafkaToHdfs;

    /**
     * 创建文件写入内容
     * @param writeName
     * @param writeContent
     * @return
     */
    @ResponseBody
    @RequestMapping("writeFile")
    public String writeFile(String writeName, String writeContent) {
        return kafkaToHdfs.wirteFile(writeName, writeContent);
    }

    /**
     * 文件上传
     * @param file
     * @return
     */
    @ResponseBody
    @RequestMapping("uploadFile")
    public String uploadFile(@RequestParam("file") MultipartFile file) {
        return kafkaToHdfs.upLoadFile(file);
    }

    /**
     * 文件下载
     * @param fileName
     * @return
     */
//    @ResponseBody
//    @RequestMapping("downLoadFile")
//    public Object downLoadFile(String fileName) {
//        return kafkaToHdfs.downLoadFile(fileName);
//    }

    /**
     * 文件删除
     * @param fileName
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "deleteFile")
    public Object deleteFile(String fileName) {
        return kafkaToHdfs.deleteFile(fileName);
    }

    /**
     * 文件列表
     * @return
     */
    @ResponseBody
    @RequestMapping("listFile")
    public List<HdfsEntity> listFile() {
        return kafkaToHdfs.listFile();
    }
    /**
     * 文件查看
     * @return
     */
    @ResponseBody
    @RequestMapping("openFile/{fileName}")
    public void openFile(HttpServletResponse response, @PathVariable("fileName") String fileName) {
        kafkaToHdfs.openFile(response, fileName);
    }

}
