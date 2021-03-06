package wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer
	extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	IntWritable ov = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		
		int sum = 0;
		for(IntWritable v : values)
			sum += v.get();
		
		ov.set(sum);
		context.write(key, ov);
	}
	
}
