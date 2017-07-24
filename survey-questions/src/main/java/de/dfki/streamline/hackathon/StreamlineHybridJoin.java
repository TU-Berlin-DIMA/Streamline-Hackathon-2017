/*
 * Copyright (C) 2017 STREAMLINE H2020
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.dfki.streamline.hackathon;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.HashMap;

// Proteus imports
//import org.apache.flink.api.common.functions.util.SideInput;
//import org.apache.flink.streaming.connectors.fs.RollingSink;


/**
 * @author behrouz
 */
public class StreamlineHybridJoin {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = createExecutionEnviromet();

        String sideInputPath = "hdfs://data/";
        final SideInput<Tuple2<String, String>> sideInput = createKeyeSideInput(sideInputPath, env);

        String host = "streamline-hackathon.de";
        Integer port = 8080;
        DataStream<Tuple3<Long, String, String>> realTimeStream = createSocketSourceInput(host, port, env);


        DataStream<Tuple4<Long, Long, String, Integer>> joinedWithBatch = realTimeStream.keyBy(1).
                map(new RichMapFunction<Tuple3<Long, String, String>, Tuple4<Long, Long, String, Integer>>() {
                    private HashMap<String, Integer> joinHM = null;

                    @Override
                    public Tuple4<Long, Long, String, Integer> map(Tuple3<Long, String, String> tuple) throws Exception {
                        if (joinHM == null) {
                            joinHM = new HashMap<>();
                            ArrayList<Tuple2<String, String>> sideRecords = (ArrayList<Tuple2<String, String>>) getRuntimeContext().getSideInput(sideInput);
                            for (Tuple2<String, String> sideTuple : sideRecords) {
                                joinHM.put(sideTuple.f0 + sideTuple.f1, joinHM.get(sideTuple.f0 + sideTuple.f1) + 1);
                            }
                        }

                        return new Tuple4<>(System.currentTimeMillis() - tuple.f0, tuple.f0, tuple.f1, joinHM.get(tuple.f1 + tuple.f2));
                    }
                }).withSideInput(sideInput);

        joinedWithBatch.print().setParallelism(1);

        env.execute();
    }

    private static StreamExecutionEnvironment createExecutionEnviromet() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(4);
        return env;
    }

    private static DataStream<Tuple3<Long, String, String>> createSocketSourceInput(String host, Integer port, StreamExecutionEnvironment env) {
        DataStream<String> socketSource = env.socketTextStream(host, port);

        return socketSource.map(new MapFunction<String, Tuple3<Long, String, String>>() {
            @Override
            public Tuple3<Long, String, String> map(String s) throws Exception {
                String[] fields = s.split(",");
                Long ts;
                try {
                    ts = new Long(fields[0]);
                } catch (Exception e) {
                    ts = -1L;
                }
                String coilID = fields[3];
                String features = "";
                for (int i = 4; i < fields.length; i++) {
                    features = features + fields[i] + ",";
                }
                return new Tuple3<Long, String, String>(ts, coilID, features);
            }
        });
    }

    private static SideInput<Tuple2<String, String>> createKeyeSideInput(String sideInputPath, StreamExecutionEnvironment env) {
        DataStream<Tuple2<String, String>> sideSource = env.readTextFile(sideInputPath).map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                String[] fields = s.split(",");
                String coilID = fields[2];
                String features = "";
                for (int i = 3; i < fields.length; i++) {
                    features = features + fields[i] + ",";

                }
                return new Tuple2<String, String>(coilID, features);
            }
        });
        return new SideInput<Tuple2<String, String>>();
        //return env.newKeyedSideInput(sideSource, 0);
    }

    static class SideInput<T> {}

}


