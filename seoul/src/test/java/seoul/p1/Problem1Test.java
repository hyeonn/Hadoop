package seoul.p1;

import org.apache.hadoop.util.ToolRunner;

public class Problem1Test {
    public static void main(String[] args) throws Exception {

        String[] myargs = {"Measurement_info.csv"};

        ToolRunner.run(new Problem1(),myargs);
    }
}
