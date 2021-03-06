package cs455.aqi.q4;

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
    Returns: <CountyCode, AQI> 
*/ 

public class Q4Mapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        // String input = value.toString();
        String[] lineSplit = value.toString().split(",");
        String countyCode = lineSplit[0];
        int aqi = Integer.parseInt(lineSplit[1]);
        long epoch = Long.parseLong(lineSplit[2]);

        Date date = new Date(epoch);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int year = calendar.get(Calendar.YEAR);

        if(year == 2020){
            context.write(new Text(countyCode), new IntWritable(aqi));
        }
    }

}


