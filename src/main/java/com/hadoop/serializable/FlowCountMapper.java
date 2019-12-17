package com.hadoop.serializable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {


    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.读取一行
        String line = value.toString();
        FlowBean v = new FlowBean();

        //2.切割字段
        String[] fields = line.split("\t");

        //3.封装对象
        String phoneNum = fields[1];
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);
        k.set(phoneNum);
        v.setDownFlow(downFlow);
        v.setUpFlow(upFlow);

        //4.写出
        context.write(k, v);
    }
}
