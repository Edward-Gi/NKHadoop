package com.test.nkhadoop.mr;

import org.apache.hadoop.conf.Configuration;

public class MR {
    static {
        // 判断是否运行在 Windows 上，如果是才设置
        if (System.getProperty("os.name").toLowerCase().indexOf("windows") != -1){
            System.setProperty("HADOOP_USER_NAME", "vagrant");
            System.setProperty("hadoop.home.dir", "c:/dev/hadoop");
        }
    }
    private static Configuration getMyConfiguration(){
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        conf.set("mapreduce.app-submission.cross-platform", "true");//跨平台提交，在windows下如果没有这句代码会报错 "/bin/bash: line 0: fg: no job control"，去网上很多都说是linux和windows环境不同导致的一般都是修改YarnRunner.java，但是其实添加了这行代码就可以了。
        conf.set("mapreduce.framework.name", "yarn");//集群的方式运行，非本地运行。
        String resourceManager="master";
        conf.set("yarn.resourcemanager.address", resourceManager + ":8032"); // 指定resourcemanager
        conf.set("yarn.resourcemanager.scheduler.address", resourceManager + ":8030");// 指定资源分配器
        conf.set("mapreduce.jobhistory.address", resourceManager + ":10020");
        // 提前规划好 jar 打包之后的位置，打包后，将 jar 复制到对应的位置，也可以使用 XxxClass.class.getClassLoader().getResource("//").toString();
        // 的方式，获取对应的位置，并拼接成对应的位置的字符串。
        // 结合 job.setJarByClass(UsersAndRatings.class)， 可以在 Windows 下和 Linux 下均可以提交运行。下述两个 key 均可以的。
        // conf.set("mapreduce.job.jar", "C:/Users/txsliwei/Desktop/Workspace/IdeaProjects/knnbymr/target/myknn_01_usersandratings.jar");
        conf.set("mapred.jar", "D:\\Code\\Java_Code\\NKHadoop\\target\\MyIndex.jar");

        return conf;
                            /*
                            //本地调试模式
                            // conf.set("mapreduce.framework.name","local");
                            // conf.set("fs.defaultFS","file:///");
                            // 本地提交模式
                            // conf.set("mapreduce.framework.name","local");
                            // conf.set("fs.defaultFS","hdfs://master:9000");
                            */
    }



}
