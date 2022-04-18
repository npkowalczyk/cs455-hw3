package cs455.aqi.q2;

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
    Mapper: Read each line of CSV data
    Grab Epoch time and AQI score
    Returns: <Month, AQI>
*/ 

public class Q2Mapper extends Mapper<Object, Text, Text, IntWritable> {

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
        int mon = date.getMonth();
        String months = "";
        switch(mon){
            case 0:
                months = "January";
                break;
            case 1:
                months = "February";
                break;
            case 2:
                months = "March";
                break;
            case 3:
                months = "April";
                break;
            case 4:
                months = "May";
                break;
            case 5:
                months = "June";
                break;
            case 6:
                months = "July";
                break;
            case 7:
                months = "August";
                break;
            case 8:
                months = "September";
                break;
            case 9:
                months = "October";
                break;
            case 10:
                months = "November";
                break;
            case 11:
                months = "December";
                break;
        }
        treeMap.put(aqi, months);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        // context write to reducer only once the mapper is done
        for (Map.Entry<Integer, String> entry : treeMap.entrySet()){
            int aqi = entry.getKey();
            String month = entry.getValue();
            context.write(new Text(month), new IntWritable(aqi));
        }
    }

}
