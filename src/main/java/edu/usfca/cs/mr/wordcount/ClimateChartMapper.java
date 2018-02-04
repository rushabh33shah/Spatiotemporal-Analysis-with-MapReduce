package edu.usfca.cs.mr.wordcount;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class ClimateChartMapper
extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        List<String> bayGeo1 = new ArrayList<>();
        bayGeo1.add("9q9jh");
        String bayGeo2 = "c3ne";

        Calendar month = Calendar.getInstance();

        String temp = value.toString();
        String[] atemp = temp.split("\t");
        String timeStamp = atemp[0];
        String geoHash = atemp[1];
        float temperature = Float.valueOf(atemp[40]);
        float rain = Float.valueOf(atemp[55]);
        month.setTimeInMillis(Long.parseLong(timeStamp));

            if (geoHash.startsWith(bayGeo2)) {
                context.write(new Text(String.valueOf(month.get(Calendar.MONTH))), new Text(String.valueOf(temperature)+"\t"+String.valueOf(rain)));
            }
    }
}
