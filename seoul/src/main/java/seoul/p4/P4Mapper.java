package seoul.p4;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class P4Mapper extends Mapper<Object, Text, Text, Text> {

    Text ok = new Text();
    Text ov = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // date,station_code,item_code,item_value
        // 2017-01-01 00:00,101,1,0.004,0
        StringTokenizer st = new StringTokenizer(value.toString(),",");
        String date = st.nextToken();
        String station_code = st.nextToken();
        String item_code = st.nextToken();
        String item_value = st.nextToken();

        StringTokenizer s = new StringTokenizer(date.toString()," ");
        s.nextToken();
        String time = s.nextToken();

        ok.set(time);
        ov.set(item_code+ "," +item_value);
        context.write(ok,ov);

    }
}
