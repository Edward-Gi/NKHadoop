package com.test.nkhadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;

public class PhoneCallMR {
    static {
        // 设置hdfs操作用户
        if(System.getProperty("os.name").toLowerCase(Locale.ROOT).indexOf("windows")!=-1)
        {
            System.setProperty("HADOOP_USER_NAME","vagrant");
            System.setProperty("hadoop.home.dir","c:/dev/hadoop");

        }
    }


    //
    public static class WCGenderMapper extends Mapper<LongWritable, Text, Text,Text>
    {
        //private final static IntWritable one = new IntWritable(1);
        public Text callfrom = new Text();
        public Text callto = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //0 获取每一行字符串，来源于输入的IsrcMap的value
            // value 是Text 类型,转换为 Java String 类型
            String line = value.toString();
            //1 将每一行文本,切分为一个个的单词
            String [] strs = line.split(" ");
            //2-1 逐个获取每一个单词
            callfrom.set(strs[0]);
            callto.set(strs[1]);
            context.write(callto,callfrom);
        }
    }
    // 在Hadoop中的可序列化的数据类型
    public static class WCGenderReducer extends Reducer<Text,Text,Text,Text>
    {
        private Text result = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 统计 key 对应的所有的value的总和
            String sum = "";
            Iterator<Text> iterator = values.iterator();
            while(iterator.hasNext()){
                Text value = iterator.next();
                String call = value.toString();
                sum = sum + " " +call;
            }
            // 最终写入最终结果
            result.set(sum);
            context.write(key,result);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //0 初始化 MR job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"MyWordCount");
        // 指明 mr 使用的类
        job.setJarByClass(PhoneCallMR.class);

        // 1 指定输入的位置 & 输入文件的类型信息
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // 2 指定 Mapper 类& 输出的key value的类型
        job.setMapperClass(WCGenderMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 3 指定 Reducer 类&最终输出的数据类型
        job.setNumReduceTasks(2);       // 默认 1, 设置为0 == 不执行 reducer
        job.setReducerClass(WCGenderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 4 指定输出的位置信息&文件类型信息
        FileOutputFormat.setOutputPath(job, new Path((args[1])));
        job.setOutputFormatClass(TextOutputFormat.class);

        // 5 提交 mrjob
        //job.submit();     //适合单一job 无反馈
        boolean result = job.waitForCompletion(true); // true 表示有反馈
        boolean issuccess =  job.isSuccessful();
        System.exit(issuccess?0:1);

    }
}
