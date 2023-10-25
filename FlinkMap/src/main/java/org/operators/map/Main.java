package org.operators.map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;


public class Main {

    public static void main(String[] args) throws Exception {
        // cobfigure flink
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // create sourse
        System.out.println("SOURCE : Read from file");
        DataSet<String> data = env.readTextFile(
                "file:///Users/oleksandrstepurko/Documents/apache-flink/input/map.txt");

        // create job
        System.out.println("Start map operation");
        DataSet<Integer> result = data.map((MapFunction<String, Integer>) value -> {
            System.out.println("Transform value : " + value);
            return Integer.parseInt(value) * Integer.parseInt(value);
        });

        // sink write to file
        System.out.println("SINK: Write to file");
        result.writeAsText(
                "file:///Users/oleksandrstepurko/Documents/apache-flink/output/map.txt",
                WriteMode.OVERWRITE);

        // excecute job
        env.execute("Operation Map");

    }
}
