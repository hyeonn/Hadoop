package seoul.p4;

import org.apache.hadoop.util.ToolRunner;

public class Problem4Test {
    public static void main(String[] args) throws Exception {

        String[] myargs = {"Measurement_info.csv"};

        ToolRunner.run(new Problem4(),myargs);
    }
}
