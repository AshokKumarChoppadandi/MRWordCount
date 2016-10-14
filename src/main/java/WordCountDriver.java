import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by cloudwick on 10/14/16.
 */
public class WordCountDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if(args.length != 2){
            System.err.println("Please Specify the Input and Output Paths");
            return -1;
        }

        Configuration conf = new Configuration();
        Job job = new Job(conf, this.getClass().toString());

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJobName("WordCountExample");
        job.setJarByClass(WordCountDriver.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        //job.setMapperClass(<MapClass>);
        job.setMapperClass(WordCountMapper.class);
        //job.setPartitionerClass(<PartitionerClass>);
        //job.setCombinerClass(<CombinerClass>);
        //job.setReducerClass(<ReducerClass>);
        job.setReducerClass(WordCountReducer.class);

        if(job.waitForCompletion(true)){
            return 0;
        }
        return 1;
    }
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new WordCountDriver(), args);
        System.exit(res);
    }
}
