package edu.usfca.cs.mr.wordcount;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class DriestMonthMapper
extends Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        List<String> bayGeo1 = new ArrayList<>();
        bayGeo1.add("9q9j");
        bayGeo1.add("9q9h");
        bayGeo1.add("9q9k");

        Calendar month = Calendar.getInstance();

        String temp = value.toString();
        String[] atemp = temp.split("\t");
        String timeStamp = atemp[0];
        String geoHash = atemp[1];
        Float atm = Float.valueOf(atemp[12]);
        month.setTimeInMillis(Long.parseLong(timeStamp));

        for (String geo: bayGeo1) {
            if (geoHash.startsWith(geo)) {
                context.write(new Text(String.valueOf(month.get(Calendar.MONTH))), new FloatWritable(atm));
            }
        }
    }
}
