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

import de.dfki.streamline.hackathon.common.BatchPayload;
import de.dfki.streamline.hackathon.common.EnrichedPayload;
import de.dfki.streamline.hackathon.common.StreamPayload;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

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

        String sideInputPath = "hdfs://cluster:9001/path/to/batch/data";
        final SideInput<BatchPayload> sideInput = createKeyedSideInput(sideInputPath, env);

        String host = "streamline-hackathon.de";
        DataStream<StreamPayload> realTimeStream = createStreamSource(host, 8080, env);


        DataStream<EnrichedPayload> joinedWithBatch = realTimeStream.keyBy(1)
            .flatMap(new RichFlatMapFunction<StreamPayload, EnrichedPayload>() {

                private MapState<Integer, EnrichedPayload> joinTable;
                private boolean isLoaded = false;

                @Override
                public void open(Configuration parameters) throws Exception {
                    super.open(parameters);
                    MapStateDescriptor<Integer, EnrichedPayload> mapDescriptor =
                            new MapStateDescriptor<>("join-table", Integer.class, EnrichedPayload.class);
                    joinTable = getRuntimeContext().getMapState(mapDescriptor);
                }

                @Override
                public void flatMap(StreamPayload value, Collector<EnrichedPayload> out) throws Exception {
                    Iterable<BatchPayload> batchData = getRuntimeContext().getSideInput(sideInput);
                    if (!isLoaded) {
                        for (BatchPayload e : batchData) {
                            joinTable.put(e.getId(), new EnrichedPayload(e));
                        }
                        isLoaded = true;
                    }
                    int key = value.getId();
                    if (joinTable.contains(key)) {
                        joinTable.put(key, joinTable.get(key).enrich(value));
                    } else {
                        joinTable.put(key, new EnrichedPayload(value));
                    }
                    out.collect(joinTable.get(key));
                }
            }).withSideInput(sideInput);


        joinedWithBatch.addSink(new SinkFunction<EnrichedPayload>() {
            @Override
            public void invoke(EnrichedPayload value) throws Exception {

            }
        });

        env.execute();
    }

    private static StreamExecutionEnvironment createExecutionEnviromet() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(4);
        return env;
    }

    private static DataStream<StreamPayload> createStreamSource(String host, Integer port, StreamExecutionEnvironment env) {
        DataStream<String> socketSource = env.socketTextStream(host, port);

        return null;
    }

    private static SideInput<BatchPayload> createKeyedSideInput(String sideInputPath, StreamExecutionEnvironment env) {
        DataStream<BatchPayload> sideSource = env.readTextFile(sideInputPath).map(new MapFunction<String, BatchPayload>() {
            @Override
            public BatchPayload map(String s) throws Exception {
                return new BatchPayload(s);
            }
        });
        return env.newKeyedSideInput(sideSource, new KeySelector<StreamPayload, Integer>() {
            @Override
            public Integer getKey(StreamPayload value) throws Exception {
                return value.getId();
            }
        });
    }

    static class SideInput<T> {}

}


