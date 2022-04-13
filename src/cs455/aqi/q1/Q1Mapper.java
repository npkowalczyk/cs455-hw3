package cs455.aqi.q1;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.util.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
    Mapper: Read each line of JSON data
    Grab Epoch time and AQI score
    Returns: 
*/ 

public class Q1Mapper extends Mapper<Object, Text, Text, IntWritable> {

    private TreeMap<Integer, String> treeMap;
 
    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        treeMap = new TreeMap<Integer, String>();
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        String input = value.toString();
        String[] lineSplit = input.split(",");
        String countyCode = lineSplit[0];
        int aqi = Integer.parseInt(lineSplit[1]);
        long epoch = Long.parseLong(lineSplit[2]);

        Date date = new Date(epoch);
        int day = date.getDay();
        String dayOfWeek = "";
        switch(day){
            case 0:
                dayOfWeek = "Sunday";
            case 1:
                dayOfWeek = "Monday";
            case 2:
                dayOfWeek = "Tuesday";
            case 3:
                dayOfWeek = "Wednesday";
            case 4:
                dayOfWeek = "Thursday";
            case 5:
                dayOfWeek = "Friday";
            case 6:
                dayOfWeek = "Saturday";
        }

        treeMap.put(aqi, dayOfWeek);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        // context write to reducer only once the mapper is done
        for (Map.Entry<Integer, String> entry : treeMap.entrySet()){
            int aqi = entry.getKey();
            String day = entry.getValue();
            context.write(new Text(day), new IntWritable(aqi));
        }
    }

}
