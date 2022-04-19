package cs455.aqi.q6;

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
    Receives: <"County,Week",  AQI>
    Sums up AQI scores for each key
    Finds mean by dividing sum by number of entries for that county
    Returns means for days of week as <"County,Week", AQIavg> 
*/

public class Q6Reducer1 extends Reducer<Text, IntWritable, Text, LongWritable> {
    private TreeMap<Long, String> weekAvgs;

    @Override 
    public void setup(Context context) throws IOException, InterruptedException{
        // creates data object that holds each day (key, value)
        weekAvgs = new TreeMap<Long, String>();
    }

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
        weekAvgs.put(avg, key.toString());

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
        // context.write
        for (Map.Entry<Long, String> entry : weekAvgs.entrySet()){
            long avg = entry.getKey();
            String info = entry.getValue();
            context.write(new Text(info), new LongWritable(avg));
        }
    }
}