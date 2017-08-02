package de.dfki.streamline.hackathon;

import de.dfki.streamline.hackathon.common.BatchPayload;
import de.dfki.streamline.hackathon.common.StreamPayload;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.common.LabeledVector;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.math.Vector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * @author behrouz
 */
public class StreamlineHybridLogisticRegression {
    static int NUMBER_OF_FEATURES = 100;
    private static Double USER_DEFINED_QUALITY_THRESHOLD = 1.0;
    private static Double MINI_BATCH_SAMPLING_RATE = 1.0;


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String sideInputPath = "hdfs://mycluster/hist.dataset";
        final SideInput<LabeledVector> sideInput = createForwardedSideInput(sideInputPath, env)
                .map(new VectorParser());

        String trainingHost = "streamline-hackathon.de/training-stream";
        DataStream<LabeledVector> trainingStream = createStreamSource(trainingHost, 8080, env)
                .map(new VectorParser());


        DataStream<Double> quality = trainingStream.keyBy(1)
                .flatMap(new RichFlatMapFunction<LabeledVector, Double>() {

                    StochasticGradientDescent stochasticGradientDescentRoutine;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        stochasticGradientDescentRoutine = new StochasticGradientDescent();
                    }

                    @Override
                    public void flatMap(LabeledVector training, Collector<Double> out) throws Exception {
                        LogisticRegressionModel model = pullModelFromParameterServer("hybrid-linear-regression");


                        Double predicted = model.predict(training.vector());
                        model.updateErrorRate(predicted, training.label());

                        Vector gradientVector = stochasticGradientDescentRoutine
                                .withInitialWeights(model.getWeights())
                                .incrementalUpdate(training)
                                .getGradientVector();

                        Double currentErrorRate = model.getErrorRate();
                        if (currentErrorRate > USER_DEFINED_QUALITY_THRESHOLD) {
                            Iterable<LabeledVector> historicalData = getRuntimeContext().getSideInput(sideInput);
                            List<LabeledVector> miniBatch = new LinkedList<>();
                            Random random = new Random();
                            for (LabeledVector trainingVector : historicalData) {
                                if (random.nextDouble() < MINI_BATCH_SAMPLING_RATE) {
                                    miniBatch.add(trainingVector);
                                }
                            }

                            miniBatch.add(training);
                            gradientVector = stochasticGradientDescentRoutine
                                    .withInitialWeights(model.getWeights().add(gradientVector))
                                    .miniBatchUpdate(miniBatch)
                                    .getGradientVector();

                        }

                        pushToParameterServer("hybrid-linear-regression", gradientVector, currentErrorRate);
                        out.collect(currentErrorRate);
                    }
                }).withSideInput(sideInput);

        quality.addSink(new SinkFunction<Double>() {
            @Override
            public void invoke(Double value) throws Exception {
            }
        });

        env.execute();
    }

    private static void pushToParameterServer(String key, Vector vector, Double errorRate) {
    }

    private static LogisticRegressionModel pullModelFromParameterServer(String key) {
        return new LogisticRegressionModel();
    }

    private static DataStream<String> createStreamSource(String host, int i, StreamExecutionEnvironment env) {
        return null;
    }

    private static class VectorParser implements MapFunction<String, LabeledVector> {

        @Override
        public LabeledVector map(String value) throws Exception {
            return new LabeledVector(0.0, DenseVector.zeros(NUMBER_OF_FEATURES));
        }
    }

    private static class LogisticRegressionModel {

        private Vector weights;
        private Double quality;

        Double predict(org.apache.flink.ml.math.Vector v) {
            return 0.0;
        }

        public Vector getWeights() {
            return weights;
        }

        public void updateErrorRate(Double predicted, double label) {

        }

        public Double getErrorRate() {
            return quality;
        }
    }

    private static SideInput<String> createForwardedSideInput(String sideInputPath, StreamExecutionEnvironment env) {
        DataStream<BatchPayload> sideSource = env.readTextFile(sideInputPath).map(new MapFunction<String, BatchPayload>() {
            @Override
            public BatchPayload map(String s) throws Exception {
                return new BatchPayload(s);
            }
        });
        return env.newForwardedSideInput(sideSource, new KeySelector<StreamPayload, Integer>() {
            @Override
            public Integer getKey(StreamPayload value) throws Exception {
                return value.getId();
            }
        });
    }

    static class StochasticGradientDescent {
        public StochasticGradientDescent withInitialWeights(Vector model) {
            return this;
        }

        public StochasticGradientDescent incrementalUpdate(LabeledVector vector) {
            return new LogisticRegressionModel();
        }

        public StochasticGradientDescent miniBatchUpdate(List<LabeledVector> vectors) {
            return new LogisticRegressionModel();
        }

        Vector getGradientVector() {
            return null;
        }
    }

    private class SideInput<T> {
        public SideInput<LabeledVector> map(VectorParser mapFunction) {
            return null;
        }
    }
}
