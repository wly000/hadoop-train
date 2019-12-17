package com.hadoop.SameFriends.fansperson;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


public class FansPersonReducer extends Reducer<Text, Text, Text, NullWritable> {


    Text k = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for (Text value : values) {
            sb.append(value.toString() + ",");
        }
        String line = key.toString() + "\t" + sb.toString();
        k.set(line);
        context.write(k, NullWritable.get());
    }
}
