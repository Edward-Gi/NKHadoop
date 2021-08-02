package com.test.nkhadoop.mr;

import org.apache.hadoop.conf.Configuration;

public class MR {
    static {
        // �ж��Ƿ������� Windows �ϣ�����ǲ�����
        if (System.getProperty("os.name").toLowerCase().indexOf("windows") != -1){
            System.setProperty("HADOOP_USER_NAME", "vagrant");
            System.setProperty("hadoop.home.dir", "c:/dev/hadoop");
        }
    }
    private static Configuration getMyConfiguration(){
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        conf.set("mapreduce.app-submission.cross-platform", "true");//��ƽ̨�ύ����windows�����û��������ᱨ�� "/bin/bash: line 0: fg: no job control"��ȥ���Ϻܶ඼˵��linux��windows������ͬ���µ�һ�㶼���޸�YarnRunner.java��������ʵ��������д���Ϳ����ˡ�
        conf.set("mapreduce.framework.name", "yarn");//��Ⱥ�ķ�ʽ���У��Ǳ������С�
        String resourceManager="master";
        conf.set("yarn.resourcemanager.address", resourceManager + ":8032"); // ָ��resourcemanager
        conf.set("yarn.resourcemanager.scheduler.address", resourceManager + ":8030");// ָ����Դ������
        conf.set("mapreduce.jobhistory.address", resourceManager + ":10020");
        // ��ǰ�滮�� jar ���֮���λ�ã�����󣬽� jar ���Ƶ���Ӧ��λ�ã�Ҳ����ʹ�� XxxClass.class.getClassLoader().getResource("//").toString();
        // �ķ�ʽ����ȡ��Ӧ��λ�ã���ƴ�ӳɶ�Ӧ��λ�õ��ַ�����
        // ��� job.setJarByClass(UsersAndRatings.class)�� ������ Windows �º� Linux �¾������ύ���С��������� key �����Եġ�
        // conf.set("mapreduce.job.jar", "C:/Users/txsliwei/Desktop/Workspace/IdeaProjects/knnbymr/target/myknn_01_usersandratings.jar");
        conf.set("mapred.jar", "D:\\Code\\Java_Code\\NKHadoop\\target\\MyIndex.jar");

        return conf;
                            /*
                            //���ص���ģʽ
                            // conf.set("mapreduce.framework.name","local");
                            // conf.set("fs.defaultFS","file:///");
                            // �����ύģʽ
                            // conf.set("mapreduce.framework.name","local");
                            // conf.set("fs.defaultFS","hdfs://master:9000");
                            */
    }



}
