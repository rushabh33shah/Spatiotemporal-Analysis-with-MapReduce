package edu.usfca.cs.mr.wordcount;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class HottestTempMapper
extends Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String temp = value.toString();
        String[] atemp = temp.split("\t");
        String TimeStamp = atemp[0];
        String geoHash = atemp[1];

        Float atm = Float.valueOf(atemp[40]);
            context.write(new Text(geoHash+TimeStamp), new FloatWritable(atm));
    }
}
