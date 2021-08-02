package com.test.nkhadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;


public class TestHDFSByAPI {

    static {
        // 设置hdfs操作用户
        System.setProperty("HADOOP_USER_NAME","vagrant");
    }

    public static void main(String[] args) throws IOException {

        // 获取句柄(操作的根目录)
        String hdfsURL = "hdfs://192.168.56.100:9000/";
        //URL url = new URL(hdfsURL);
        URI uri = URI.create(hdfsURL);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(uri,conf);

    }
}
