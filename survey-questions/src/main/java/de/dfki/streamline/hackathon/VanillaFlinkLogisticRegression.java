package de.dfki.streamline.hackathon;

import de.dfki.streamline.hackathon.common.EOFMarker;
import de.dfki.streamline.hackathon.common.TextInputFormatWithEOF;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.common.LabeledVector;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.math.Vector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * @author behrouz
 */
public class VanillaFlinkLogisticRegression {
    static int NUMBER_OF_FEATURES = 100;
    private static Double USER_DEFINED_QUALITY_THRESHOLD = 1.0;
    private static Double MINI_BATCH_SAMPLING_RATE = 1.0;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // load the batch file
        String pathToBatchFile = "hdfs://mycluster/hist.dataset";

        TextInputFormatWithEOF format = new TextInputFormatWithEOF(new Path(pathToBatchFile), FilePathFilter.createDefaultFilter());

        TypeInformation<Either<String, EOFMarker>> typeInfo = TypeInformation.of(new TypeHint<Either<String, EOFMarker>>() {});

        DataStream<Either<LabeledVector, EOFMarker>> historicalBatchData =
                env.readFile(format, pathToBatchFile, FileProcessingMode.PROCESS_ONCE, -1, typeInfo)
                        .map(new MapFunction<Either<String, EOFMarker>, Either<LabeledVector, EOFMarker>>() {
                            @Override
                            public Either<LabeledVector, EOFMarker> map(Either<String, EOFMarker> value) throws Exception {
                                if (value.isLeft()) {
                                    return Either.Left(new VectorParser().parse(value.left()));
                                } else {
                                    return Either.Right(value.right());
                                }
                            }
                        });

        String trainingHost = "streamline-hackathon.de/training-stream";
        DataStream<LabeledVector> trainingStream = createStreamSource(trainingHost, 8080, env)
                .map(new VectorParser());

        DataStream<Either<LabeledVector, EOFMarker>> batchData = new DataStream<>(env,
                new PartitionTransformation<>(historicalBatchData.getTransformation(),
                        new StreamPartitioner<Either<LabeledVector, EOFMarker>>() {

                            int[] returnArray;
                            boolean set;
                            int setNumber;

                            @Override
                            public String toString() {
                                return "FANCY-PART";
                            }

                            @Override
                            public StreamPartitioner<Either<LabeledVector, EOFMarker>> copy() {
                                return this;
                            }

                            @Override
                            public int[] selectChannels(SerializationDelegate<StreamRecord<Either<LabeledVector, EOFMarker>>> record, int numChannels) {
                                Either<LabeledVector, EOFMarker> data = record.getInstance().getValue();
                                if (data.isLeft()) {
                                    return new int[]{KeyGroupRangeAssignment.assignKeyToParallelOperator(new Random().nextInt(), KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM, numChannels)};
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

        DataStream<Double> quality = trainingStream.connect(batchData).flatMap(new HybridModelTrainer());

        quality.addSink(new SinkFunction<Double>() {
            @Override
            public void invoke(Double value) throws Exception {
            }
        });

        env.execute();
    }

    public static class HybridModelTrainer extends RichCoFlatMapFunction<LabeledVector, Either<LabeledVector, EOFMarker>, Double> {

        private ListState<LabeledVector> historicalDataSet;
        private ListState<LabeledVector> streamBuffer;
        private ValueState<Boolean> isBatchConsumed;
        private LogisticRegressionModel model;
        private StochasticGradientDescent stochasticGradientDescentRoutine;


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            ListStateDescriptor<LabeledVector> historicalDataDescriptor =
                    new ListStateDescriptor<>("historical-dataset", LabeledVector.class);

            ListStateDescriptor<LabeledVector> streamBufferDescriptor =
                    new ListStateDescriptor<>("stream-buffer", LabeledVector.class);

            ValueStateDescriptor<Boolean> valDescriptor = new ValueStateDescriptor<>("batch-consumed", Boolean.class);

            historicalDataSet = getRuntimeContext().getListState(historicalDataDescriptor);
            streamBuffer = getRuntimeContext().getListState(streamBufferDescriptor);
            isBatchConsumed = getRuntimeContext().getState(valDescriptor);

            stochasticGradientDescentRoutine = new StochasticGradientDescent();
        }

        @Override
        public void flatMap1(LabeledVector training, Collector<Double> out) throws Exception {
            if (isBatchConsumed.value() == null) {
                isBatchConsumed.update(false);
            }
            model = pullModelFromParameterServer("hybrid-linear-regression");

            Double predicted = model.predict(training.vector());
            model.updateErrorRate(predicted, training.label());

            Vector gradientVector = stochasticGradientDescentRoutine
                    .withInitialWeights(model.getWeights())
                    .incrementalUpdate(training)
                    .getGradientVector();

            Double currentErrorRate = model.getErrorRate();


            if (currentErrorRate > USER_DEFINED_QUALITY_THRESHOLD) {
                if (!isBatchConsumed.value()) {
                    streamBuffer.add(training);
                } else {
                    List<LabeledVector> miniBatch = new LinkedList<>();
                    Random random = new Random();
                    for (LabeledVector trainingVector : historicalDataSet.get()) {
                        if (random.nextDouble() < MINI_BATCH_SAMPLING_RATE) {
                            miniBatch.add(trainingVector);
                        }
                    }
                    for (LabeledVector labeledVector : streamBuffer.get()) {
                        miniBatch.add(labeledVector);
                    }
                    streamBuffer.clear();

                    gradientVector = stochasticGradientDescentRoutine
                            .withInitialWeights(model.getWeights().add(gradientVector))
                            .miniBatchUpdate(miniBatch)
                            .getGradientVector();
                }

            }

            Iterator<LabeledVector> it = streamBuffer.get().iterator();
            if (!it.hasNext()) {
                pushToParameterServer("hybrid-linear-regression", gradientVector, currentErrorRate);
                out.collect(currentErrorRate);
            }
        }

        @Override
        public void flatMap2(Either<LabeledVector, EOFMarker> training, Collector<Double> out) throws Exception {
            if (isBatchConsumed.value() == null) {
                isBatchConsumed.update(false);
            }
            if (training.isRight()) {
                isBatchConsumed.update(true);
            } else {
                historicalDataSet.add(training.left());
            }
        }
    }

    private static DataStream<String> createStreamSource(String host, int i, StreamExecutionEnvironment env) {
        return null;
    }

    private static class VectorParser implements MapFunction<String, LabeledVector> {

        @Override
        public LabeledVector map(String value) throws Exception {
            return new LabeledVector(0.0, DenseVector.zeros(NUMBER_OF_FEATURES));
        }

        LabeledVector parse(String value) {
            return null;
        }
    }

    static class StochasticGradientDescent {
        StochasticGradientDescent withInitialWeights(Vector model) {
            return this;
        }

        StochasticGradientDescent incrementalUpdate(LabeledVector vector) {
            return new StochasticGradientDescent();
        }

        StochasticGradientDescent miniBatchUpdate(List<LabeledVector> vectors) {
            return new StochasticGradientDescent();
        }

        Vector getGradientVector() {
            return null;
        }
    }

    private static class LogisticRegressionModel {

        private Vector weights;
        private Double quality;

        Double predict(org.apache.flink.ml.math.Vector v) {
            return 0.0;
        }

        Vector getWeights() {
            return weights;
        }

        void updateErrorRate(Double predicted, double label) {

        }

        Double getErrorRate() {
            return quality;
        }
    }

    private static void pushToParameterServer(String key, Vector vector, Double errorRate) {
    }

    private static LogisticRegressionModel pullModelFromParameterServer(String key) {
        return new LogisticRegressionModel();
    }
}
