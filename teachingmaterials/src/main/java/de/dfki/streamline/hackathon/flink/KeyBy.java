package de.dfki.streamline.hackathon.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author behrouz
 */
public class KeyBy {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> integers = env.fromElements(1, 2, 3, 4);

        KeyedStream<Tuple2<String, Integer>, Tuple> tuples = integers.map(
                new MapFunction<Integer, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Integer value){
                        if (value % 2 == 0) {
                            return new Tuple2<>("even", value);
                        } else {
                            return new Tuple2<>("odd", value);
                        }
                    }
                })
                .keyBy(0);

        tuples.print();
        env.execute();
    }
}
