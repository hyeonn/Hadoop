package wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	Text ok = new Text();
	IntWritable ov = new IntWritable(1);
	
	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer st = new StringTokenizer(value.toString());
		
		while(st.hasMoreTokens()) {
			String word = st.nextToken();
			ok.set(word);
			context.write(ok, ov);
		}
		
	}
}
