package com.hadoop.inputforamt;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;


public class SequenceFileDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"C:\\Users\\Leahy\\Desktop\\data\\input_sequence", "C:\\Users\\Leahy\\Desktop\\data\\output_sequence"};

        //1 获取配置信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2 设置业务的驱动类和业务的map和reduce类
        job.setJarByClass(SequenceFileDriver.class);
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        //7 设置自定义的FileInputFormat
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //3 设置map阶段的输出kv格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        //4 设置最终输出的kv格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        //5 设置任务提交文件的输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //6 提交作业
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);



    }
}
