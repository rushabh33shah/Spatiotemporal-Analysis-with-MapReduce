package edu.usfca.cs.mr.wordcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SolarWindReducer
extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(
            Text key,Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

        double highwind = 0;
        double lowcloud = 999999;
        double average = 0;
        int i = 0;
        int j=0;
        double wind;

        for(Text val: values){

            String temp = val.toString();
            String[] atemp = temp.split("\t");

            String windVComp = atemp[0];
            String windUComp = atemp[1];
            String cloudCover = atemp[2];
            double fwindV = Float.parseFloat(windVComp);
            double fwindU = Float.parseFloat(windUComp);
            float fcloudCover = Float.parseFloat(cloudCover);

            wind=Math.sqrt(Math.pow(fwindU,2)+Math.pow(fwindV,2));

            if (wind>highwind){
                highwind=wind;
                i++;
            }
            if (fcloudCover>0) {
                if (fcloudCover < lowcloud) {
                    lowcloud = fcloudCover;
                    j++;
                }
            }
        }
        average = highwind/i;
        double averain = lowcloud/j;
        context.write(key, new Text(String.valueOf(average)+"\t"+String.valueOf(averain)+"\t"+String.valueOf(average+averain)));
    }
}
