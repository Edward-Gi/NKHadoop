package com.test.nkhadoop.hdfs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

// ������ HDFS: /input/core-site.xml
// hdfs://192.168.56.100:9000/input/core-site.xml
public class TestHDFS {
    // ���� HDFS Э��֧�֣���֤ URL �������Ӳ���hdfs�ϵ�һ����
    // ������jvm��ע��һ�����Է���static code��
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }
    // ָ��hdfsЭ���ʽ��url,������hosts�ļ�����master��ipaddressӳ�䣬rpc�˿���9000
    public static String hdfsURL="hdfs://192.168.56.100:9000/input/core-site.xml";

    public static void main(String[] args) throws IOException {
        URL url = new URL(hdfsURL);
        // ��ȡ������������hdfsЭ���IOStream
        //InputStream is = url.openStream(); ���Ƽ�
        // ��ȡ url ���ӣ����������ӻ�ȡ IOStream�� ���� IOStream �Ϳ�����ɶ�д(�� HDFS���ϴ�����)
        URLConnection conn = url.openConnection();
        InputStream is = conn.getInputStream();

        // ���� hadoop �Ĺ�����ʵ��IOStream �Ĳ���
        IOUtils.copyBytes(is,System.out,4096,false);

        IOUtils.closeStream(is); // is.close()
    }
}
