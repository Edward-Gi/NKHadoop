package com.test.nkhadoop.mr;

import com.test.nkhadoop.mr.entity.MovieInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class Movie {
    static {
        // 设置hdfs操作用户
        if(System.getProperty("os.name").toLowerCase(Locale.ROOT).indexOf("windows")!=-1)
        {
            System.setProperty("HADOOP_USER_NAME","vagrant");
            System.setProperty("hadoop.home.dir","c:/dev/hadoop");

            // 设置没有空格|中文路径的的临时目录
            //System.setProperty("hadoop.tmp","d:/mrtmp");
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
        conf.set("mapred.jar", "D:\\Code\\Java_Code\\NKHadoop\\target\\Movie.jar");

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

    public static class MovieMap extends Mapper<LongWritable,Text, Text, MovieInfo>
    {
        public static String MovieIDText = "";
        public final static Map<String,MovieInfo> MovieRatingMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 加载movie.dat数据
            String uriStr = "hdfs://master:9000/input/mr/movie/movie.dat";
            FileSystem fs = FileSystem.get(URI.create(uriStr),context.getConfiguration());   // 加载文件系统
            FSDataInputStream fdis = fs.open(new Path(uriStr));    // 打开文件系统下相应路径，获取数据输入流
            BufferedReader br = new BufferedReader(new InputStreamReader(fdis));   //将数据输入流转化为字符流

            //
            String line = br.readLine();
            String [] fields = line.split("::");
            while (line!=null)
            {
                String MovieID = fields[0];
                MovieIDText = MovieID;
                String Title = fields[1];
                String Genres = fields[2];
                MovieInfo movieInfo = new MovieInfo();
                movieInfo.setMovieID(MovieID);
                movieInfo.setTitle(Title);
                movieInfo.setGenres(Genres);
                MovieRatingMap.put(movieInfo.getMovieID(),movieInfo);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = null;
            String [] fields = line.split("::");
            
        }
    }
}
