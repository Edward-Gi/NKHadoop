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
        // ����hdfs�����û�
        if(System.getProperty("os.name").toLowerCase(Locale.ROOT).indexOf("windows")!=-1)
        {
            System.setProperty("HADOOP_USER_NAME","vagrant");
            System.setProperty("hadoop.home.dir","c:/dev/hadoop");

            // ����û�пո�|����·���ĵ���ʱĿ¼
            //System.setProperty("hadoop.tmp","d:/mrtmp");
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
        conf.set("mapred.jar", "D:\\Code\\Java_Code\\NKHadoop\\target\\Movie.jar");

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

    public static class MovieMap extends Mapper<LongWritable,Text, Text, MovieInfo>
    {
        public static String MovieIDText = "";
        public final static Map<String,MovieInfo> MovieRatingMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // ����movie.dat����
            String uriStr = "hdfs://master:9000/input/mr/movie/movie.dat";
            FileSystem fs = FileSystem.get(URI.create(uriStr),context.getConfiguration());   // �����ļ�ϵͳ
            FSDataInputStream fdis = fs.open(new Path(uriStr));    // ���ļ�ϵͳ����Ӧ·������ȡ����������
            BufferedReader br = new BufferedReader(new InputStreamReader(fdis));   //������������ת��Ϊ�ַ���

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
