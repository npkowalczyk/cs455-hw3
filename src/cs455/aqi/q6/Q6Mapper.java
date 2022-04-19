package cs455.aqi.q6;

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
    Mapper: Read each line of csv data
    Grab County Code, Epoch time and AQI score
    Returns: <"CountyCode,Week", AQI> 
*/ 

public class Q6Mapper1 extends Mapper<Object, Text, Text, IntWritable> {

    private TreeMap<String, Integer> treeMap;
 
    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        treeMap = new TreeMap<String, Integer>();
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        // parse csv file
        String[] lineSplit = value.toString().split(",");
        String countyCode = lineSplit[0];
        int aqi = Integer.parseInt(lineSplit[1]);
        long epoch = Long.parseLong(lineSplit[2]);

        // create date object and get week number
        Date date = new Date(epoch);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int weekNumber = calendar.get(Calendar.WEEK_OF_YEAR);

        String info = countyCode + "," + weekNumber;

        treeMap.put(info, aqi);

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        // context write to reducer only once the mapper is done
        for (Map.Entry<String, Integer> entry : treeMap.entrySet()){
            String info = entry.getKey();
            int aqi = entry.getValue();
            context.write(new Text(info), new IntWritable(aqi));
        }
    }

}