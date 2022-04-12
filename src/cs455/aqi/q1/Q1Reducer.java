package cs455.aqi.q1;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.util.TreeMap;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
    Receives: <Day, AQI Scores>
    Sums up AQI scores for each key (day of week)
    Finds mean by dividing sum by number of entries for that day
    Returns means for days of week as <Day, mean> 
*/

public class Q1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private TreeMap<Text, Int> DaysAvg;

    @Override 
    public void setup(Context context) throws IOException, InterruptedException{
        // creates data object that holds each day (key, value)
        DaysAvg = new TreeMap<Int, String>();
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        // sum of all aqi scores
        int sum = 0;
        // keeps track of number of items for key
        int num = 0;
        for(IntWritable val : values){
            num += 1;
            sum += val.get();
        }
        int avg = sum / num;
        // add average for individual day to TreeMap
        DaysAvg.put(avg, key.toString());
        print(avg, key.toString());
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
        // context.write
        for (Map.Entry<Int, String> entry : DaysAvg.entrySet()){
            int avg = entry.getKey();
            String day = entry.getValue();
            context.write(new Text(day), new IntWritable(avg));
        }
    }
}