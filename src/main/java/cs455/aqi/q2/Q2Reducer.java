package cs455.aqi.q2;

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
    Receives: <Month, AQI Scores>
    Sums up AQI scores for each key (month)
    Finds mean by dividing sum by number of entries for that month
    Returns means for month as <Month, mean> 
*/

public class Q2Reducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    // private TreeMap<Long, String> MonthAvg;
    // private TreeMap<Long, String> finalOutput;

    // @Override 
    // public void setup(Context context) throws IOException, InterruptedException{
    //     // creates data object that holds each day (key, value)
    //     MonthAvg = new TreeMap<Long, String>();
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
        context.write(new Text(key.toString()), new DoubleWritable(avg));

    }

    // @Override
    // public void cleanup(Context context) throws IOException, InterruptedException{
    //     // context.write
    //     for (Map.Entry<Long, String> entry : finalOutput.entrySet()){
    //         long avg = entry.getKey();
    //         String month = entry.getValue();
    //         context.write(new Text(month), new LongWritable(avg));
    //     }
    // }
}