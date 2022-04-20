package cs455.aqi.q3;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.util.TreeMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
    Receives: <CountyCode, AQI Score>
    Sums up AQI scores for each county 
    Finds mean by dividing sum by number of entries for that county
    Returns means for days of week as <County, AQI mean> 
*/

public class Q3Reducer extends Reducer<Text, IntWritable, Text, LongWritable> {
    private TreeMap<Long, String> CountyAvg = new TreeMap<Long, String>();

    // @Override 
    // public void setup(Context context) throws IOException, InterruptedException{
    //     // creates data object that holds each day (key, value)
    //     CountyAvg = new TreeMap<Long, String>();
    // }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        // sum of all aqi scores
        long sum = 0;
        // keeps track of number of items for key
        int num = 0;
        long avg = 0;
        for(IntWritable val : values){
            num += 1;
            sum += val.get();
        }
        avg = sum / num;
        // add average for individual day to TreeMap
        CountyAvg.put(avg, key.toString());
        
        // trim map to best 10 average AQI scores 
        if(CountyAvg.size() > 10){
            CountyAvg.remove(CountyAvg.firstKey());
        }

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
        // context.write
        for (Map.Entry<Long, String> entry : CountyAvg.entrySet()){
            long avg = entry.getKey();
            String countyCode = entry.getValue();
            context.write(new Text(countyCode), new LongWritable(avg));
        }
    }
}
