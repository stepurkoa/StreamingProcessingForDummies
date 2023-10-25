package org.flink.wc;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class Main {

    public static void main(String[] args) throws Exception {
        // Configure flink job
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // create sourse
        DataSet<String> text = env.readTextFile(
                "file:///Users/oleksandrstepurko/Documents/apache-flink/input/wc.txt");

        // job pipepline
        DataSet<String> filtered = text.filter(new FilterFunction<String>() {
//            public boolean filter(String value) {
//                return value.startsWith("N");
//            }
            public boolean filter(String value) {
                return value.length() > 0;
            }
        });
        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());

        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(new int[]{0}).sum(1);


        //crearte sink
        counts.writeAsCsv(
                "file:///Users/oleksandrstepurko/Documents/apache-flink/output/wc_out.txt",
                "\n", " ");

        env.execute("WordCount Example");

    }

    public static final class Tokenizer
            implements MapFunction<String, Tuple2<String, Integer>> {

        public Tuple2<String, Integer> map(String value) {
            return new Tuple2(value, Integer.valueOf(1));
        }
    }
}
