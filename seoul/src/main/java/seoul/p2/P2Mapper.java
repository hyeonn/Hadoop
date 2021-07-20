package seoul.p2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class P2Mapper extends Mapper<Object, Text, IntWritable, Text> {

    IntWritable ok = new IntWritable();
    Text ov = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // date,station_code,item_code,item_value
        // 2017-01-01 00:00,101,1,0.004,0
        StringTokenizer st = new StringTokenizer(value.toString(),",");
        String date = st.nextToken();
        int station_code = Integer.parseInt(st.nextToken());
        String item_code = st.nextToken();
        double item_value = Double.parseDouble(st.nextToken());


        if(item_code.equals("8") && item_value<=30){
            ok.set(station_code);
            ov.set(date + "," + item_code);
            context.write(ok,ov);
        }
        else if(item_code.equals("9") && item_value<=15){
            ok.set(station_code);
            ov.set(date + "," + item_code);
            context.write(ok,ov);
        }

    }
}
