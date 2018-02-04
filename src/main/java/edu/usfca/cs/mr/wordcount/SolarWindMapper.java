package edu.usfca.cs.mr.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class SolarWindMapper
extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        List<String> bayGeo1 = new ArrayList<>();
        Calendar month = Calendar.getInstance();
        String temp = value.toString();
        String[] atemp = temp.split("\t");
        String timeStamp = atemp[0];
        String geoHash = atemp[1];
        float windVComp = Float.valueOf(atemp[37]);
        float windUComp = Float.valueOf(atemp[53]);
        float cloudCover = Float.valueOf(atemp[16]);
        float land = Float.valueOf(atemp[18]);
        month.setTimeInMillis(Long.parseLong(timeStamp));

        bayGeo1.add("9r");
        bayGeo1.add("9q");
        bayGeo1.add("9m");
        bayGeo1.add("9x");
        bayGeo1.add("9w");
        bayGeo1.add("9t");
        bayGeo1.add("9z");
        bayGeo1.add("9y");
        bayGeo1.add("9v");
        bayGeo1.add("dp");
        bayGeo1.add("dn");
        bayGeo1.add("dj");
        bayGeo1.add("dr");
        bayGeo1.add("dq");
        bayGeo1.add("c2");
        bayGeo1.add("c8");
        bayGeo1.add("cb");

        if (land>0){
            for (String geo: bayGeo1) {
                if (geoHash.startsWith(geo)){
                    context.write(new Text(geoHash), new Text(String.valueOf(windVComp)
                            + "\t" + String.valueOf(windUComp) + "\t" + String.valueOf(cloudCover)));
                }
            }
        }
    }
}
