package seoul.p2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class P2Reducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {

    IntWritable ok = new IntWritable();
    IntWritable ov = new IntWritable();

    // 좋음이 가장 많이 측정된 KEY, VALUE 저장하는 변수
    int maxK = 0;
    int maxV = 0;

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values,
                          Context context) throws IOException, InterruptedException {
        ArrayList days = new ArrayList();
        int cnt = 0;

        for(Text t : values) {
            StringTokenizer st = new StringTokenizer(t.toString(),",");
            String date = st.nextToken();
            String item_code = st.nextToken();

            // 같은 date가 두 번 오면 PM10,PM2.5 모두 좋은 상태
            if(days.contains(date)){
                cnt += 1;
                days.remove(date);
            }
            else {
                days.add(date);
            }

        }

        // 최댓값보다 크면 변경
        if(maxV<cnt){
            maxK = key.get();
            maxV = cnt;
        }

        // 마지막에 가장 많이 측정된 지역 write
        if(key.get()==125){
            ok.set(maxK);
            ov.set(maxV);
            context.write(ok, ov);
        }


    }
}
