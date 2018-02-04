package edu.usfca.cs.mr.wordcount;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DriestMonthReducer
extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<FloatWritable> values, Context context)
    throws IOException, InterruptedException {

        float count = 0;
        int i = 0;
        boolean flag = false;

        for(FloatWritable val : values){
            if (val.get()>0) {
                count += val.get();
                i++;
                flag=true;
            }
            else {
                flag=false;
                continue;
            }
        }
        float avg = count/i;
        if (flag) {
            context.write(key, new FloatWritable(avg));
        }
    }

}
