package seoul.p2;

import org.apache.hadoop.util.ToolRunner;


public class Problem2Test {
    public static void main(String[] args) throws Exception {

        String[] myargs = {"Measurement_info.csv"};

        ToolRunner.run(new Problem2(),myargs);
    }
}
