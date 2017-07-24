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
import de.dfki.streamline.hackathon.common.EOFMarker;
import de.dfki.streamline.hackathon.common.EnrichedPayload;
import de.dfki.streamline.hackathon.common.StreamPayload;
import de.dfki.streamline.hackathon.common.TextInputFormatWithEOF;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class VanillaFlinkHybridJoin {


	public static class PayloadSchema implements DeserializationSchema<StreamPayload>, SerializationSchema<StreamPayload> {

		@Override
		public StreamPayload deserialize(byte[] message) throws IOException {
			return null;
		}

		@Override
		public boolean isEndOfStream(StreamPayload nextElement) {
			return false;
		}

		@Override
		public TypeInformation<StreamPayload> getProducedType() {
			return TypeInformation.of(new TypeHint<StreamPayload>() {});
		}

		@Override
		public byte[] serialize(StreamPayload element) {
			return new byte[0];
		}
	}

	public static class BatchStreamingJoin extends RichCoFlatMapFunction<StreamPayload, Either<BatchPayload, EOFMarker>, EnrichedPayload> {

		private MapState<Integer, EnrichedPayload> joinTable;
		private ValueState<Boolean> isBatchConsumed;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			MapStateDescriptor<Integer, EnrichedPayload> mapDescriptor =
					new MapStateDescriptor<>("join-table", Integer.class, EnrichedPayload.class);

			ValueStateDescriptor<Boolean> valDescriptor = new ValueStateDescriptor<>("batch-consumed", Boolean.class);

			joinTable = getRuntimeContext().getMapState(mapDescriptor);
			isBatchConsumed = getRuntimeContext().getState(valDescriptor);
		}

		@Override
		public void flatMap1(StreamPayload value, Collector<EnrichedPayload> out) throws Exception {
			int key = value.getId();
			if (isBatchConsumed.value() == null) {
				isBatchConsumed.update(false);
			}
			if (joinTable.contains(key)) {
				joinTable.put(key, joinTable.get(key).enrich(value));
			} else {
				joinTable.put(key, new EnrichedPayload(value));
			}
			flushLastIfConsumed(key, out);
		}

		@Override
		public void flatMap2(Either<BatchPayload, EOFMarker> value, Collector<EnrichedPayload> out) throws Exception {
			if (isBatchConsumed.value() == null) {
				isBatchConsumed.update(false);
			}
			if (value.isRight()) {
				isBatchConsumed.update(true);
				flushAll(out);
			} else {
				BatchPayload data = value.left();
				int key = data.getId();
				if (joinTable.contains(key)) {
					joinTable.put(key, joinTable.get(key).enrich(data));
				} else {
					joinTable.put(key, new EnrichedPayload(data));
				}
				flushLastIfConsumed(key, out);
			}
		}


		void flushAll(final Collector<EnrichedPayload> out) throws Exception {
			Iterator<Map.Entry<Integer, EnrichedPayload>> it = joinTable.iterator();
			while (it.hasNext()) {
				out.collect(it.next().getValue());
			}
		}

		void flushLastIfConsumed(int key, final Collector<EnrichedPayload> out) throws Exception {
			if (isBatchConsumed.value()) {
				out.collect(joinTable.get(key));
			}
		}

	}


	public static void main(String[] args) throws Exception {

		Properties kafkaProps = new Properties();

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// load the batch file

		String pathToBatchFile = "/path/to/batch/data";

		TextInputFormatWithEOF format = new TextInputFormatWithEOF(new Path(pathToBatchFile), FilePathFilter.createDefaultFilter());

		TypeInformation<Either<String, EOFMarker>> typeInfo = TypeInformation.of(new TypeHint<Either<String, EOFMarker>>() {});


		DataStream<Either<BatchPayload, EOFMarker>> parsedBatchData =
				env.readFile(format, pathToBatchFile, FileProcessingMode.PROCESS_ONCE, -1, typeInfo)
					.map(new MapFunction<Either<String, EOFMarker>, Either<BatchPayload, EOFMarker>>() {
						@Override
						public Either<BatchPayload, EOFMarker> map(Either<String, EOFMarker> value) throws Exception {
							if (value.isLeft()) {
								return Either.Left(new BatchPayload(value.left()));
							} else {
								return Either.Right(value.right());
							}
						}
		});


		DataStream<Either<BatchPayload, EOFMarker>> batchData = new DataStream<>(env,
			new PartitionTransformation<>(parsedBatchData.getTransformation(),
			new StreamPartitioner<Either<BatchPayload, EOFMarker>>() {

				int[] returnArray;
				boolean set;
				int setNumber;

				@Override
				public String toString() {
					return "FANCY-PART";
				}

				@Override
				public StreamPartitioner<Either<BatchPayload, EOFMarker>> copy() {
					return this;
				}

				@Override
				public int[] selectChannels(SerializationDelegate<StreamRecord<Either<BatchPayload, EOFMarker>>> record, int numChannels) {
					Either<BatchPayload, EOFMarker> data = record.getInstance().getValue();
					if (data.isLeft()) {
						return new int[] { KeyGroupRangeAssignment.assignKeyToParallelOperator(data.left().getId(), KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM, numChannels) };
					} else {
						if (set && setNumber == numChannels) {
							return returnArray;
						} else {
							this.returnArray = new int[numChannels];
							for (int i = 0; i < numChannels; i++) {
								returnArray[i] = i;
							}
							set = true;
							setNumber = numChannels;
							return returnArray;
						}
					}
				}
			}
		));


		DataStream < StreamPayload > stream = env.addSource(new FlinkKafkaConsumer010<>("stream-topic", new PayloadSchema(), kafkaProps))
						.keyBy(new KeySelector<StreamPayload, Integer>() {
							@Override
							public Integer getKey(StreamPayload value) throws Exception {
								return value.getId();
							}
						});


		DataStream<EnrichedPayload> intermediate = stream.connect(batchData).flatMap(new BatchStreamingJoin());



		intermediate.addSink(new SinkFunction<EnrichedPayload>() {
			@Override
			public void invoke(EnrichedPayload value) throws Exception {

			}
		});

		String queryPlan = env.getExecutionPlan();
		env.execute();


	}



}


