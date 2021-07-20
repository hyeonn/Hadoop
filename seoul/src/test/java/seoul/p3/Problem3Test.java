package seoul.p3;

import org.apache.hadoop.util.ToolRunner;

public class Problem3Test {
    public static void main(String[] args) throws Exception {

        String[] myargs = {"Measurement_info.csv"};

        ToolRunner.run(new Problem3(),myargs);
    }
}
