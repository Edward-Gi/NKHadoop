package com.test.nkhadoop.hdfs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

// 操作： HDFS: /input/core-site.xml
// hdfs://192.168.56.100:9000/input/core-site.xml
public class TestHDFS {
    // 开启 HDFS 协议支持，保证 URL 可以连接并打开hdfs上的一个流
    // 秩序在jvm中注册一次所以放在static code中
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }
    // 指定hdfs协议格式的url,可以在hosts文件配置master和ipaddress映射，rpc端口是9000
    public static String hdfsURL="hdfs://192.168.56.100:9000/input/core-site.xml";

    public static void main(String[] args) throws IOException {
        URL url = new URL(hdfsURL);
        // 获取输入流，基于hdfs协议的IOStream
        //InputStream is = url.openStream(); 不推荐
        // 获取 url 连接，并基于连接获取 IOStream， 基于 IOStream 就可以完成读写(对 HDFS的上传下载)
        URLConnection conn = url.openConnection();
        InputStream is = conn.getInputStream();

        // 借助 hadoop 的工具类实现IOStream 的操作
        IOUtils.copyBytes(is,System.out,4096,false);

        IOUtils.closeStream(is); // is.close()
    }
}
