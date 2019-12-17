package com.hadoop.SameFriends.personfans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class PersonFansDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //设置输入输出路径
        args = new String[]{"C:\\Users\\Leahy\\Desktop\\data\\input_commonfriends\\MapReduce2", "C:\\Users\\Leahy\\Desktop\\data\\input_commonfriends\\MapReduce2_out"};

        //1.获取配置信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.制定本程序使用的jar包所在的路径
        job.setJarByClass(PersonFansDriver.class);

        //3.指定本业务mapper和reducer业务类
        job.setMapperClass(PersonFansMapper.class);
        job.setReducerClass(PersonFansReducer.class);

        //4.指定mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //5.指定最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //6.指定job输入输出的原始路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //虽然自定义了输出的outputformat但是还需要输出_SUCCESS文件，
        // 还是得要一个由FileOuFormat指定的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7.提交作业
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
