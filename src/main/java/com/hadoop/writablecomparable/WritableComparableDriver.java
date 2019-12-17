package com.hadoop.writablecomparable;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WritableComparableDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //设置输入输出路径
        args = new String[]{"C:\\Users\\Leahy\\Desktop\\data\\input_writablecomparable", "C:\\Users\\Leahy\\Desktop\\data\\output_writablecomparable"};

        //1.获取配置信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.制定本程序使用的jar包所在的路径
        job.setJarByClass(WritableComparableDriver.class);

        //3.指定本业务mapper和reducer业务类
        job.setMapperClass(WritableComparableMapper.class);
        job.setReducerClass(WritableComparableReducer.class);

        //4.指定mapper输出的kv类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //5.指定最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

//        job.setPartitionerClass(ProvincePartitioner.class);
//        job.setNumReduceTasks(5);

        //6.指定job输入输出的原始路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7.提交作业
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
