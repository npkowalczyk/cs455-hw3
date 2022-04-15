package cs455.aqi.q2;

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

public class Q2Job{
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "q2");
        // current class
        job.setJarByClass(Q2Job.class);
        // Mapper
        job.setMapperClass(Q2Mapper.class);
        // Combiner
        //job.setCombinerClass(IntSumReducer.class);
        // Reducer
        job.setReducerClass(Q2Reducer.class);
        // Outputs from the Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // Outputs from the Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        // set number of tasks
        job.setNumReduceTasks(1); 
        System.out.println(args[0]);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }      
}