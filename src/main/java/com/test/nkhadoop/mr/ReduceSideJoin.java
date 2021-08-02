package com.test.nkhadoop.mr;

import com.test.nkhadoop.mr.entity.JoinBean;
import org.apache.hadoop.conf.Configuration;
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class ReduceSideJoin {

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
        conf.set("mapred.jar", "D:\\Code\\Java_Code\\NKHadoop\\target\\ReduceSide.jar");

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

    public static class ReduceSideJoinMapper
        // �ڶ���Text ��product ID ������,������ҪproductID,���Խ����ΪHadoop�е�null---NullWritable
        extends Mapper<LongWritable, Text, Text, JoinBean>
    {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            String[] fields = line.split(",");
            String orderID = "";
            String productID = "";
            String category = "";
            String amount = "";
            String flag = "";

            // �ж�������Դ���Ӷ�������ֵĸ���
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String datasourceFilename = inputSplit.getPath().getName();

            if (datasourceFilename != null && datasourceFilename.indexOf("orderdetails") != -1) {
                orderID = fields[0];
                productID = fields[1];
                amount = fields[2];
                flag = "orderdetails";      // ��Դ�� orderdetails
            } else if (datasourceFilename != null && datasourceFilename.indexOf("categories") != -1) {
                productID = fields[0];
                category = fields[1];
                flag = "categories";        // ��Դ�� categories
            }
            JoinBean joinBean = new JoinBean(orderID, productID, category, amount, flag);
            // ��������Ĺ��е�productID��ΪMidMap��key,��joinBean������Ϊvalue
            context.write(new Text(productID), joinBean);
        }

    }
    private static class ReduceSlideJoinReducer
            extends Reducer<Text,JoinBean,Text,JoinBean>
    {
        @Override
        protected void reduce(Text key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException
        {
            String category ="";
            List<JoinBean> detailBeans = new ArrayList<>();
            for(JoinBean bean:values)
            {
                if(bean.getFlag().equals("categories"))
                {
                    category = bean.getCategory();
                }
                else
                {
                    detailBeans.add(new JoinBean(bean.getOrderID(), bean.getProductID(), bean.getCategory(),bean.getAmount(),"2"));
                }
            }
            for(JoinBean bean:detailBeans)
            {
                bean.setCategory(category);
                context.write(new Text(bean.getProductID()),bean);
            }
            // detailBean:{[]}
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
        job.setJobName("ReduceSideJob");
        job.setJarByClass(ReduceSideJoin.class);

        // 3 �������� λ��&����
        // ����ļ����룬ָ��λ��
        for(int i=0;i<remianingArgs.length-1;i++)
        {
            FileInputFormat.addInputPath(job,new Path(remianingArgs[i]));
        }
        job.setInputFormatClass(TextInputFormat.class);

        // 4 ���� mapper -- ��&����� key&value ������
        job.setMapperClass(ReduceSideJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinBean.class);

        // 5 ���� reducer -- ��&�����key&value ������
        job.setNumReduceTasks(2);//
        job .setReducerClass(ReduceSlideJoinReducer.class);
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
