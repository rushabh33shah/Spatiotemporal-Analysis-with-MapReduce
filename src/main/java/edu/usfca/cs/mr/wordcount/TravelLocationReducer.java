package edu.usfca.cs.mr.wordcount;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TravelLocationReducer
extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<FloatWritable> values, Context context)
    throws IOException, InterruptedException {

        float count = 0;
        // calculate the total count
        int i = 0;
        for(FloatWritable val : values){
            count += val.get();
            i++;
        }
        float avg = count/i;
        if (avg>300 && avg<310) {
            context.write(key, new FloatWritable(count));
        }
    }

}
