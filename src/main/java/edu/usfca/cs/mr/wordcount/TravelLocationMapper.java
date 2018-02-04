package edu.usfca.cs.mr.wordcount;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class TravelLocationMapper
extends Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        List<String> bayGeo1 = new ArrayList<>();
        bayGeo1.add("9qjj");//Las Vegas
        //bayGeo1.add("9qdz");//Yosemite
        //bayGeo1.add("9qep");//Yosemite
        bayGeo1.add("9qdy");//Yosemite
        //bayGeo1.add("9qen");//Yosemite
        bayGeo1.add("9q94");//Santa Cruz
        //bayGeo1.add("9q96");//Santa Cruz
        bayGeo1.add("9q5b");//Long Beach
        bayGeo1.add("9mud");//San Diego
        //bayGeo1.add("9muf");//San Diego
        //bayGeo1.add("9muc");//San Diego
        bayGeo1.add("87zc");//Honolulu

        Calendar month = Calendar.getInstance();

        String temp = value.toString();
        String[] atemp = temp.split("\t");
        String timeStamp = atemp[0];
        String geoHash = atemp[1];
        Float atm = Float.valueOf(atemp[40]);
        month.setTimeInMillis(Long.parseLong(timeStamp));

        for (String geo: bayGeo1) {
             if (geoHash.startsWith(geo))
                context.write(new Text(String.valueOf(month.get(Calendar.MONTH))+" - "+geo), new FloatWritable(atm));
        }
    }
}
