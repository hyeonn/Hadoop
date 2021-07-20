package seoul.p3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class P3Reducer extends Reducer<Text, Text, Text, Text> {

    Text ov = new Text();
    //Double.parseDouble(st.nextToken())

    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Context context) throws IOException, InterruptedException {



        String SO2 = new String();
        String NO2 = new String();
        String CO = new String();
        String O3 = new String();
        String PM10 = new String();
        String PM25 = new String();

        for(Text t : values) {
            StringTokenizer st = new StringTokenizer(t.toString(),",");
            int item_code = Integer.parseInt(st.nextToken());
            String item_value = st.nextToken();
            if (item_code==1){
                SO2 = item_value;
            }
            else if (item_code==3){
                NO2 = item_value;
            }
            else if (item_code==5){
                CO = item_value;
            }
            else if (item_code==6){
                O3 = item_value;
            }
            else if (item_code==8){
                PM10 = item_value;
            }
            else if (item_code==9){
                PM25 = item_value;
            }
        }
        String s = new String("1"+"\t"+SO2+"\t"+"3"+"\t"+NO2+"\t"+"5"+"\t"+CO+"\t"+"6"+"\t"+O3+"\t"+"8"+"\t"+PM10+"\t"+"9"+"\t"+PM25);
        ov.set(s);
        context.write(key, ov);
    }
}
