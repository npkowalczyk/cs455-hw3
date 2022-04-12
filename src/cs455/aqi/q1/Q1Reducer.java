package cs455.aqi.q1;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
    Receives: 
    Sums up AQI scores for each key (day of week)
    Finds mean by dividing sum by number of entries for that day
    Returns means for days of week as <key, mean> 
*/


public class Q1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    // @Override 
    // public void setup(Context context) throws IOException, InterruptedException{
    //     // creates data object that holds each day (key, value)

    // }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        // gets 7 keys corresponding to the days of the week
        // checks if data object has certain day of week (key)
            // If it does, add to that and get average
        // basically, check data object and update it
        try{
            // sum of all aqi scores
            int sum = 0;
            // keeps track of number of items for key
            int num = 0;
            for(IntWritable val : values){
                num += 1;
                sum += val.get();
            }
            int avg = sum / num;
            context.write(key, new IntWritable(avg));
            print(key, avg);

        } catch(Exception e){
            e.printStackTrace();
        }

    }

    // @Override
    // public void cleanup(Context context) throws IOException, InterruptedException{
    //     // context.write
    

    // }
}