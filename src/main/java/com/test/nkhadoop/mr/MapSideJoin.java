package com.test.nkhadoop.mr;

import com.test.nkhadoop.mr.entity.JoinBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class MapSideJoin {

    static {
        // ����hdfs�����û�
        if(System.getProperty("os.name").toLowerCase(Locale.ROOT).indexOf("windows")!=-1)
        {
            System.setProperty("HADOOP_USER_NAME","vagrant");
            System.setProperty("hadoop.home.dir","c:/dev/hadoop");

            // ����û�пո�|����·���ĵ���ʱĿ¼
            //System.setProperty("hadoop.tmp","d:/mrtmp");
        }
    }// �����������������

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
        conf.set("mapred.jar", "D:\\Code\\Java_Code\\NKHadoop\\target\\MapSide.jar");

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

    public static class MapJoinMapper
        extends Mapper<LongWritable,Text,Text,JoinBean>
    {
        private final Text productIDText = new Text();
        private final static Map<String,JoinBean> joinBeanMap= new HashMap<>();

        // ��mapper֮ǰִ�У���ִֻ��һ��
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // categories�Ĵ洢·��
            String uriStr = "hdfs://master:9000/input/mr/joinbean/categories";
            FileSystem fs = FileSystem.get(URI.create(uriStr),context.getConfiguration());
            FSDataInputStream fdis = fs.open(new Path(uriStr));
            BufferedReader br = new BufferedReader(new InputStreamReader(fdis));

            // ����categories����Map<productID,JoinBean>
            String line = null;
            String [] fields = null;
            while ((line = br.readLine())!=null)
            {
                fields = line.split(",");
                JoinBean joinBean = new JoinBean();
                joinBean.setProductID(fields[0]);
                joinBean.setCategory(fields[1]);
                joinBeanMap.put(joinBean.getProductID(),joinBean);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String [] fields = line.split(",");
            if(fields.length < 3)
            {
                return;
            }
            String productID = fields[1];
            JoinBean joinBean = joinBeanMap.get(productID);
            if(joinBean==null)
            {
                return;
            }
            String orderID = fields[0];
            String amount = fields[2];
            joinBean.setOrderID(orderID);
            joinBean.setAmount(amount);

            productIDText.set(productID);
            context.write(productIDText,joinBean);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // ������ȡConfiguration
        //Configuration configuration = new Configuration();
        Configuration configuration = getMyConfiguration();

        // 1 ��ȡ�����в��� -- ������Ϣ
        GenericOptionsParser genericOptionsParser = new GenericOptionsParser(configuration, args);
        String [] remianingArgs = genericOptionsParser.getRemainingArgs();
        if (remianingArgs.length<2)
        {
            System.out.println("Usage: MRJobname <in> [<in>...], <out>");
            System.exit(2);
        }

        // -- MR ���
        // 2 ��������ʼ�� MR Job
        Job job = Job.getInstance(configuration);
        job.setJobName("MapSideJob");
        job.setJarByClass(MapSideJoin.class);

        // 3 �������� λ��&����
        // ����ļ����룬ָ��λ��
        for(int i=0;i<remianingArgs.length-1;i++)
        {
            FileInputFormat.addInputPath(job,new Path(remianingArgs[i]));
        }
        job.setInputFormatClass(TextInputFormat.class);

        // 4 ���� mapper -- ��&����� key&value ������
        job.setMapperClass(MapSideJoin.MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinBean.class);

        // 5 ���� reducer -- ��&�����key&value ������
        job.setNumReduceTasks(0);// ��ִ�� Reducer
        //job .setReducerClass(ReduceSlideJoinReducer.class);

        // ����Reducer�����Ҳ����������
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(JoinBean.class);

        // 6 ������� -- λ��&����
        FileOutputFormat.setOutputPath(job,new Path(remianingArgs[remianingArgs.length-1]));
        job.setOutputFormatClass((TextOutputFormat.class));

        // 7 �ύ
        job.waitForCompletion(true);
        boolean result = job.isSuccessful();
        System.exit(result?0:1);


    }
}
