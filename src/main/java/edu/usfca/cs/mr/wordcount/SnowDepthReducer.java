package edu.usfca.cs.mr.wordcount;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SnowDepthReducer
extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<FloatWritable> values, Context context)
    throws IOException, InterruptedException {

        float count = 0;
        boolean flag = true;
        int i =0;

        for(FloatWritable val : values){
            if (val.get()>0) {
                count += val.get();
                i++;
            }
            else {
                flag=false;
                continue;
            }
        }
        if (flag) {
            float avg= count/i;
            context.write(key, new FloatWritable(avg));
        }
    }
}
