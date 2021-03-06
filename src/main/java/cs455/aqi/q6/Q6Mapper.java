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
    Mapper: Read each line of JSON data
    Grab Epoch time and AQI score
    Returns: <Day, AQI> 
*/ 

public class Q6Mapper extends Mapper<Object, Text, Text, IntWritable> {

    //private TreeMap<Integer, String> treeMap = new TreeMap<Integer, String>();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        String input = value.toString();
        String[] lineSplit = input.split(",");
        String countyCode = lineSplit[0];
        int aqi = Integer.parseInt(lineSplit[1]);
        long epoch = Long.parseLong(lineSplit[2]);
        
        Date date = new Date(epoch);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int year = calendar.get(Calendar.YEAR);

        String firstThree = countyCode.substring(0, 3);
        String state = "";

        if (year == 2020){
            switch(firstThree){
                case "G06":
                    state = "California";
                    break;
                case "G08":
                    state = "Colorado";
                    break;
                case "G12":
                    state = "Florida";
                    break;
                case "G13":
                    state = "Georgia";
                    break;
                case "G26":
                    state = "Michigan";
                    break;
                case "G42":
                    state = "Pennsylvania";
                    break;
                case "G48":
                    state = "Texas";
                    break;
                case "G53":
                    state = "Washington";
                    break;
            }
            context.write(new Text(state), new IntWritable(aqi));
        }
    }

}
