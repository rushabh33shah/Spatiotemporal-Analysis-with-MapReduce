package edu.usfca.cs.mr.wordcount;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ClimateChartReducer
extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(
            Text key,Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

        float high = 0;
        float low = 999999;
        float average = 0;
        float counttemp=0;
        float countrain=0;
        int i = 0;
        int j=0;

        for(Text val: values){

            String temp = val.toString();
            String[] atemp = temp.split("\t");

            String temperature = atemp[0];
            String rain = atemp[1];
            float ftemp = Float.parseFloat(temperature);
            float frain = Float.parseFloat(rain);

            if (ftemp>0.0) {
                if (ftemp > high) {
                    high = ftemp;
                }
                if (ftemp < low) {
                    low = ftemp;
                }
                counttemp += ftemp;
                i++;
            }

            if (frain>0.0){
                countrain += frain;
                j++;
            }
        }
        average = counttemp/i;
        float averain = countrain/j;
        context.write(key, new Text(String.valueOf(high)+"\t"+String.valueOf(low)+"\t"+String.valueOf(averain)+"\t"+String.valueOf(average)));
    }
}
