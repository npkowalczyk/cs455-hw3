package cs455.aqi.q1;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.util.TreeMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
    Receives: <Day, AQI Scores>
    Sums up AQI scores for each key (day of week)
    Finds mean by dividing sum by number of entries for that day
    Returns means for days of week as <Day, mean> 
*/

public class Q1Reducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    // private TreeMap<Long, String> DaysAvg;
    // private TreeMap<Long, String> finalOutput;

    // @Override 
    // public void setup(Context context) throws IOException, InterruptedException{
    //     // creates data object that holds each day (key, value)
    //     DaysAvg = new TreeMap<Long, String>();
    //     //finalOutput = new TreeMap<Long, String>();
    // }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        // sum of all aqi scores
        long sum = 0;
        // keeps track of number of items for key
        int num = 0;
        double avg = 0;
        for(IntWritable val : values){
            num += 1;
            sum += val.get();
        }
        avg = sum / num;
        // add average for individual day to TreeMap
        //DaysAvg.put(avg, key.toString());


        context.write(new Text(key.toString()), new DoubleWritable(avg));
        // output best and worst AQIf
        //finalOutput.put(DaysAvg.lastKey(), DaysAvg.get(DaysAvg.lastEntry()));
        //finalOutput.put(DaysAvg.firstKey(), DaysAvg.get(DaysAvg.firstEntry()));

    }

    // @Override
    // public void cleanup(Context context) throws IOException, InterruptedException{
    //     // context.write
    //     for (Map.Entry<Long, String> entry : finalOutput.entrySet()){
    //         long avg = entry.getKey();
    //         String day = entry.getValue();
    //         context.write(new Text(day), new LongWritable(avg));
    //     }
    // }
}