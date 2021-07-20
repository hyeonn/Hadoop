package seoul.p1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class P1Mapper extends Mapper<Object, Text, Text, DoubleWritable> {

    Text ok = new Text();
    DoubleWritable ov = new DoubleWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // date,station_code,item_code,item_value
        // 2017-01-01 00:00,101,1,0.004,0
        StringTokenizer st = new StringTokenizer(value.toString(),",");
        st.nextToken();
        String station_code = st.nextToken();
        String item_code = st.nextToken();
        double item_value = Double.parseDouble(st.nextToken());

        if(item_code.equals("8")){
            ok.set(station_code);
            ov.set(item_value);
            context.write(ok,ov);
        }

    }
}
