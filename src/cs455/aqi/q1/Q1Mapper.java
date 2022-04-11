package cs455.aqi.q1;

import java.io.IOException;
import java.sql.Date;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
    Mapper: Read each line of JSON data
    Grab Epoch time and AQI score
    Returns: 

    //convert epoch time to timestamp into String(text obj)
    //key: day of week, AQI score
    //return 7 key:val pairs
*/ 


public class Q1Mapper extends Mapper<Object, Text, Text, IntWritable> {


    private Text weekDay = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        String input = value.toString();
        String[] lineSplit = input.split(",");
        String countyCode = lineSplit[0];
        int aqi = Integer.parseInt(lineSplit[1]);
        long epoch = Long.parseLong(lineSplit[2]);

        Date date = new Date(epoch);
        int day = date.getDay();
        String dayOfWeek;
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
        
        context.write(dayOfWeek, aqi );


    }
}
