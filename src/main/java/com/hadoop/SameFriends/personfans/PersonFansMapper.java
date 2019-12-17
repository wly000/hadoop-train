package com.hadoop.SameFriends.personfans;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;


public class PersonFansMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text v = new Text();
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");
        String fan = fields[0];
        String[] persons = fields[1].split(",");
        Arrays.sort(persons);

        for (int i = 0; i < persons.length - 1; i++) {
            for (int j = i + 1; j < persons.length; j++) {
                StringBuffer sb = new StringBuffer();
                sb.append(persons[i] + "——>" + persons[j]);
                k.set(sb.toString());
                v.set(fan);
                context.write(k, v);
            }
        }
    }
}
