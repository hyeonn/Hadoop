package seoul.p4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class P4Reducer extends Reducer<Text, Text, Text, DoubleWritable> {

    Text ok = new Text();
    DoubleWritable ov = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Context context) throws IOException, InterruptedException {

        double SO2 = 0.00;
        double NO2 = 0.00;
        double CO = 0.00;
        double O3 = 0.00;
        double PM10 = 0.00;
        double PM25 = 0.00;

        int cnt1 = 0;
        int cnt3 = 0;
        int cnt5 = 0;
        int cnt6 = 0;
        int cnt8 = 0;
        int cnt9 = 0;


        for(Text t : values) {
            StringTokenizer st = new StringTokenizer(t.toString(),",");
            int item_code = Integer.parseInt(st.nextToken());
            double item_value = Double.parseDouble(st.nextToken());
            if (item_code==1){
                SO2 += item_value;
                cnt1 += 1;
            }
            else if (item_code==3){
                NO2 += item_value;
                cnt3 += 1;
            }
            else if (item_code==5){
                CO += item_value;
                cnt5 += 1;
            }
            else if (item_code==6){
                O3 += item_value;
                cnt6 += 1;
            }
            else if (item_code==8){
                PM10 += item_value;
                cnt8 += 1;
            }
            else if (item_code==9){
                PM25 += item_value;
                cnt9 += 1;
            }
        }

        // SO2
        String s = new String(key.toString()+"\t"+"1");
        ok.set(s);
        ov.set(SO2/cnt1);
        context.write(ok, ov);

        // NO2
        s = key.toString()+"\t"+"3";
        ok.set(s);
        ov.set(NO2/cnt3);
        context.write(ok, ov);

        // CO
        s = key.toString()+"\t"+"5";
        ok.set(s);
        ov.set(CO/cnt5);
        context.write(ok, ov);

        // O3
        s = key.toString()+"\t"+"6";
        ok.set(s);
        ov.set(O3/cnt6);
        context.write(ok, ov);

        // PM10
        s = key.toString()+"\t"+"8";
        ok.set(s);
        ov.set(PM10/cnt8);
        context.write(ok, ov);

        // PM2.5
        s = key.toString()+"\t"+"9";
        ok.set(s);
        ov.set(PM25/cnt9);
        context.write(ok, ov);


    }
}
