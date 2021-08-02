package com.test.nkhadoop.mr;

import com.huaban.analysis.jieba.JiebaSegmenter;
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
import java.util.List;
import java.util.Locale;

public class WordCount {
    static {
        // ����hdfs�����û�
        if(System.getProperty("os.name").toLowerCase(Locale.ROOT).indexOf("windows")!=-1)
        {
            System.setProperty("HADOOP_USER_NAME","vagrant");
            System.setProperty("hadoop.home.dir","c:/dev/hadoop");

        }
    }

    public static class WCGenderMapper extends Mapper<LongWritable, Text, Text,IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        public Text word = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //0 ��ȡÿһ���ַ�������Դ�������IsrcMap��value
            // value ��Text ����,ת��Ϊ Java String ����
            String line = value.toString();
            //1 ��ÿһ���ı�,�з�Ϊһ�����ĵ���
            //String [] strs = line.split(" ");
            //2-1 �����ȡÿһ������
//            for(String str : strs)
//            {
//                //2-2 �����mapper�������map(midMap<���е�ÿһ������,1>)
//                word.set(str);
//                context.write(word,one);
//            }
            JiebaSegmenter jiebaSegmenter = new JiebaSegmenter();
            List<String> words = jiebaSegmenter.sentenceProcess(line);
            for(String w:words)
            {
                word.set(w);
                context.write(word,one);
            }
        }
    }
    // �ֱ��Ӧ mapper �������key��value �����ս����key,value
    // ��Hadoop�еĿ����л�����������
    public static class WCGenderReducer extends Reducer<Text,IntWritable,Text,LongWritable>
    {
        private LongWritable result = new LongWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // ͳ�� key ��Ӧ�����е�value���ܺ�
            long sum =0;
            Iterator<IntWritable> iterator = values.iterator();
            while(iterator.hasNext()){
                IntWritable value = iterator.next();
                sum += value.get();    // ���� one = 1 ==> sum++;
            }
            // ����д�����ս��
            result.set(sum);
            context.write(key,result);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //0 ��ʼ�� MR job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"MyWordCount");
        // ָ�� mr ʹ�õ���
        job.setJarByClass(WordCount.class);

        // 1 ָ�������λ�� & �����ļ���������Ϣ
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // 2 ָ�� Mapper ��& �����key value������
        job.setMapperClass(WCGenderMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 3 ָ�� Reducer ��&�����������������
        job.setNumReduceTasks(2);       // Ĭ�� 1, ����Ϊ0 == ��ִ�� reducer
        job.setReducerClass(WCGenderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 4 ָ�������λ����Ϣ&�ļ�������Ϣ
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // 5 �ύ mrjob
        //job.submit();     //�ʺϵ�һjob �޷���
        boolean result = job.waitForCompletion(true); // true ��ʾ�з���
        boolean issuccess =  job.isSuccessful();
        System.exit(issuccess?0:1);

    }
}
