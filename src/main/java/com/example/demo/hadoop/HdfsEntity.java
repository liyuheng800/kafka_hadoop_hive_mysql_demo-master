package com.example.demo.hadoop;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Date;

@Data
public class HdfsEntity {

    private String name;

    private long size;

    private String date;

    private int num;

    public HdfsEntity() {
    }

    public HdfsEntity(String name, long size, String date, int num) {
        this.name = name;
        this.size = size;
        this.date = date;
        this.num = num;
    }
}
