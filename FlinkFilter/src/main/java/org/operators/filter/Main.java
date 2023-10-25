package org.operators.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;

public class Main {

    public static void main(String[] args) throws Exception {
        // configure flink job
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // get sourse
        System.out.println("SOURCE : Read from file");
        DataSet<String> data = env.readTextFile(
                "file:///Users/oleksandrstepurko/Documents/apache-flink/input/filter.txt");

        // run job
        System.out.println("Start map operation");
        FilterOperator<String> result = data.flatMap(
                (FlatMapFunction<String, String>) (value, out) -> {
                    for (String word : value.split(" ")) {
                        out.collect(word);
                    }
                }).returns(Types.STRING)
                .filter((FilterFunction<String>) value -> {
                    int i = Integer.parseInt(value);
                    return i % 2 == 0;
                });

        // ctrete sink
        System.out.println("SINK: Write to file");
        result.writeAsText(
                "file:///Users/oleksandrstepurko/Documents/apache-flink/output/filter.txt",
                WriteMode.OVERWRITE);

        // execute job
        env.execute("Operation FlatMap + Filter");
    }
}
