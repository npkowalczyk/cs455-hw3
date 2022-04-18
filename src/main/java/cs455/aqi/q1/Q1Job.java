package cs455.aqi.q1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class Q1Job{
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "q1");
        // current class
        job.setJarByClass(Q1Job.class);
        // Mapper
        job.setMapperClass(Q1Mapper.class);
        // Combiner
        //job.setCombinerClass(IntSumReducer.class);
        // Reducer
        job.setReducerClass(Q1Reducer.class);
        // Outputs from the Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // Outputs from the Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);

        job.setNumReduceTasks(1); 
        System.out.println(args[0]);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }      
}