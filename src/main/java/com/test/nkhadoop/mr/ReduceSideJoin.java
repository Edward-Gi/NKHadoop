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
        // 设置hdfs操作用户
        if(System.getProperty("os.name").toLowerCase(Locale.ROOT).indexOf("windows")!=-1)
        {
            System.setProperty("HADOOP_USER_NAME","vagrant");
            System.setProperty("hadoop.home.dir","c:/dev/hadoop");

            // 设置没有空格|中文路径的的临时目录
            //System.setProperty("hadoop.tmp","d:/mrtmp");
        }
    }// 声明出错的行数变量

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
        conf.set("mapred.jar", "D:\\Code\\Java_Code\\NKHadoop\\target\\ReduceSide.jar");

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

    public static class ReduceSideJoinMapper
        // 第二个Text 是product ID 的类型,若不想要productID,可以将其改为Hadoop中的null---NullWritable
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

            // 判断数据来源，从而决定拆分的个数
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String datasourceFilename = inputSplit.getPath().getName();

            if (datasourceFilename != null && datasourceFilename.indexOf("orderdetails") != -1) {
                orderID = fields[0];
                productID = fields[1];
                amount = fields[2];
                flag = "orderdetails";      // 来源于 orderdetails
            } else if (datasourceFilename != null && datasourceFilename.indexOf("categories") != -1) {
                productID = fields[0];
                category = fields[1];
                flag = "categories";        // 来源于 categories
            }
            JoinBean joinBean = new JoinBean(orderID, productID, category, amount, flag);
            // 用两个表的共有的productID作为MidMap的key,将joinBean容器作为value
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
        // 创建获取Configuration
        //Configuration configuration = new Configuration();
        Configuration configuration = getMyConfiguration();

        // 1 获取命令行参数 -- 运行信息
        GenericOptionsParser genericOptionsParser = new GenericOptionsParser(configuration, args);
        String [] remianingArgs = genericOptionsParser.getRemainingArgs();
        if (remianingArgs.length<2)
        {
            System.out.println("Usage: MRJobname <in> [<in>...], <out>");
            System.exit(2);
        }

        // -- MR 相关
        // 2 创建并初始化 MR Job
        Job job = Job.getInstance(configuration);
        job.setJobName("ReduceSideJob");
        job.setJarByClass(ReduceSideJoin.class);

        // 3 设置输入 位置&类型
        // 多个文件输入，指定位置
        for(int i=0;i<remianingArgs.length-1;i++)
        {
            FileInputFormat.addInputPath(job,new Path(remianingArgs[i]));
        }
        job.setInputFormatClass(TextInputFormat.class);

        // 4 设置 mapper -- 类&输出的 key&value 的类型
        job.setMapperClass(ReduceSideJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinBean.class);

        // 5 设置 reducer -- 类&输出的key&value 的类型
        job.setNumReduceTasks(2);//
        job .setReducerClass(ReduceSlideJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(JoinBean.class);

        // 6 设置输出 -- 位置&类型
        FileOutputFormat.setOutputPath(job,new Path(remianingArgs[remianingArgs.length-1]));
        job.setOutputFormatClass((TextOutputFormat.class));

        // 7 提交
        job.waitForCompletion(true);
        boolean result = job.isSuccessful();
        System.exit(result?0:1);


    }
}
