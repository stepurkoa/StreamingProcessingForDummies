package org.operators.flatmap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
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
                "file:///Users/oleksandrstepurko/Documents/apache-flink/input/flatMap.txt");

        System.out.println("Start map operation");
        FlatMapOperator<String, String> result = data.flatMap(
                (FlatMapFunction<String, String>) (value, out) -> {
                    for (String word : value.split(" ")) {
                        out.collect(word);
                    }
                }).returns(Types.STRING);

        System.out.println("SINK: Write to file");
        result.writeAsText(
                "file:///Users/oleksandrstepurko/Documents/apache-flink/output/flatMap.txt",
                WriteMode.OVERWRITE);

        env.execute("Operation FlatMap");

    }
}
