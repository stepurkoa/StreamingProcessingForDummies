package org.operators.reduce;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;

public class Main {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env
                = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        System.out.println("SOURCE : Read from file");
        DataSet<String> data = env.readTextFile(
                "file:///Users/oleksandrstepurko/Documents/apache-flink/input/filter.txt");

        System.out.println("Start flat map operation");
        ReduceOperator<Integer> result = data.flatMap(
                        (FlatMapFunction<String, String>) (value, out) -> {
                            for (String word : value.split(" ")) {
                                out.collect(word);
                            }
                        }).returns(Types.STRING)
                .map((MapFunction<String, Integer>) Integer::parseInt)
                .filter((FilterFunction<Integer>) value -> value % 2 == 0)
                .reduce((ReduceFunction<Integer>) Integer::sum);

        System.out.println("SINK: Write to file");
        result.writeAsText(
                "file:///Users/oleksandrstepurko/Documents/apache-flink/output/reduce.txt",
                WriteMode.OVERWRITE);

        env.execute("Operation FlatMap + Filter");

    }
}
