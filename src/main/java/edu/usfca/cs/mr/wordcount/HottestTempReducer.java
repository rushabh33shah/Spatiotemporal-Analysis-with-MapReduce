package edu.usfca.cs.mr.wordcount;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HottestTempReducer
extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<FloatWritable> values, Context context)
    throws IOException, InterruptedException {
        float count = 0;

        for(FloatWritable val : values){
            if (count<val.get()) {
                count = val.get();
            }
        }
        context.write(key, new FloatWritable(count));
    }
}
