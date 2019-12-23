package com.example.demo.hadoop;

import net.minidev.json.JSONObject;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Optional;

@Component
public class HDFSTools {

    public void HdfsWrite(Object object)throws Exception{
        if (StringUtils.isEmpty(object)){
            throw new RuntimeException("Data cannot be null！");
        }

        HDFSApp.setUp();
        HDFSApp.mkdir();
        HDFSApp.create(object);
    }


}
