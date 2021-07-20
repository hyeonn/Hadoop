package seoul.p2;

//Driver

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

public class Problem2 extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Problem2(), args);
    }

    public int run(String[] args) throws Exception {

        String inputPath = args[0];
        String outputPath = args[0] + "2.out";

        Job job = Job.getInstance(getConf());
        job.setJarByClass(Problem2.class);

        job.setMapperClass(P2Mapper.class);
        job.setReducerClass(P2Reducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);

        return 0;
    }

}
