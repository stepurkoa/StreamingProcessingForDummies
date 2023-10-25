package org.flink.window;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TumblingWindowExample {

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define a list of predefined values
        List<Long> predefinedValues = new ArrayList<>();
        predefinedValues.add(10L);
        predefinedValues.add(20L);
        predefinedValues.add(30L);

        List<String> predefinedKeys = new ArrayList<>();
        predefinedKeys.add("user_1");
        predefinedKeys.add("user_2");
        predefinedKeys.add("user_3");

        // Create a custom source that generates values from the list at a rate of 2 messages per second
        DataStream<Tuple3<String, Long, Long>> dataStream = env
                .addSource(new CustomSource(predefinedKeys, predefinedValues))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, Long, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, timestamp) -> event.f2));

        // Define a tumbling window of 10 seconds
        DataStream<Tuple2<String, Long>> resultStream = dataStream
                .keyBy((KeySelector<Tuple3<String, Long, Long>, Object>) value -> value.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) // window size in seconds
                .sum(1) // tuple position
                .map((MapFunction<Tuple3<String, Long, Long>, Tuple2<String, Long>>) value -> new Tuple2<>(value.f0 + "_aggr", value.f1))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // Print the result to stdout
        resultStream.print();

        // Execute the Flink job
        env.execute("Tumbling Window Example");
    }

    // Custom SourceFunction to generate values from a list at 2 messages per second
    public static class CustomSource implements SourceFunction<Tuple3<String, Long, Long>> {

        private volatile boolean isRunning = true;
        private final List<Long> predefinedValues;
        private final List<String> predefinedKeys;
        private final Random random;

        public CustomSource(List<String> predefinedKeys, List<Long> predefinedValues) {
            this.predefinedKeys = predefinedKeys;
            this.predefinedValues = predefinedValues;
            this.random = new Random();
        }

        @Override
        public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            while (isRunning) {
                String key = predefinedKeys.get(getIndex(predefinedKeys.size()));
                long value = predefinedValues.get(getIndex(predefinedValues.size()));
                Tuple3<String, Long, Long> tuple = new Tuple3<>(key, value,
                        System.currentTimeMillis());
                System.out.println(tuple);
                ctx.collect(tuple);
                Thread.sleep(500); // Sleep for 500 milliseconds to generate 2 messages per second
            }
        }

        private int getIndex(Integer size) {
            return random.nextInt(size);
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
