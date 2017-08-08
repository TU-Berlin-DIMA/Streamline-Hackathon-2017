package de.dfki.streamline.hackathon.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author behrouz
 */
public class Reduce {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> integers = env.fromElements(1, 2, 3, 4);

        DataStream<Tuple2<String, Integer>> tuples = integers.map(new MapFunction<Integer, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Integer value) throws Exception {
                if (value % 2 == 0) {
                    return new Tuple2<>("even", value);
                } else {
                    return new Tuple2<>("odd", value);
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> odd_and_evens = tuples.keyBy(0);
        DataStream<Tuple2<String, Integer>> sums = odd_and_evens
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                    }
                });
        sums.print();
        env.execute();

    }

}
