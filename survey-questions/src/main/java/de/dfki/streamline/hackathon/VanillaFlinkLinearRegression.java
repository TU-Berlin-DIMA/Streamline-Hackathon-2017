package de.dfki.streamline.hackathon;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.common.LabeledVector;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author behrouz
 */
public class VanillaFlinkLinearRegression {
    static int NUMBER_OF_FEATURES = 100;

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, LabeledVector>> trainingDataSet = batchEnv.readTextFile("hdfs://mycluster/hist.dataset")
                .map(new VectorParser());

        LinearRegressionSGD regressor = new LinearRegressionSGD()
                .withInitWeights(DenseVector.zeros(NUMBER_OF_FEATURES), 0.0)
                .withNumIterations(100);

        RegressionModel batchRegressionModel = regressor.fit(trainingDataSet);

        batchEnv.execute();

        StreamExecutionEnvironment streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        String trainingHost = "streamline-hackathon.de/training-stream";
        DataStream<Tuple2<Integer, LabeledVector>> trainingStream = createStreamSource(trainingHost, 8080, streamingEnv)
                .map(new VectorParser());

        String testHost = "streamline-hackathon.de/test-stream";
        DataStream<Tuple2<Integer, LabeledVector>> testStream = createStreamSource(testHost, 8080, streamingEnv)
                .map(new VectorParser());

        StreamingLinearRegressionSGD streamRegressor = new StreamingLinearRegressionSGD()
                .withInitWeights(batchRegressionModel.getWeightVector(), batchRegressionModel.getIntercept());

        RegressionModel model = streamRegressor.fit(trainingStream);

        streamRegressor.predict(model, testStream);

        streamingEnv.execute();
    }

    private static DataStream<String> createStreamSource(String host, int i, StreamExecutionEnvironment env) {
        return null;
    }

    private static class VectorParser implements MapFunction<String, Tuple2<Integer, LabeledVector>> {
        @Override
        public Tuple2<Integer, LabeledVector> map(String value) throws Exception {
            return new Tuple2<>(0, new LabeledVector(0.0, DenseVector.zeros(NUMBER_OF_FEATURES)));
        }
    }

    private static class LinearRegressionSGD {
        LinearRegressionSGD withInitWeights(DenseVector zeros, double intercept) {
            return this;
        }

        LinearRegressionSGD withNumIterations(int iterations) {
            return this;
        }

        public RegressionModel fit(DataSet<Tuple2<Integer, LabeledVector>> trainingDataSet) {
            return new RegressionModel();
        }
    }

    private static class StreamingLinearRegressionSGD {

        StreamingLinearRegressionSGD withInitWeights(DenseVector zeros, double v) {
            return this;
        }


        RegressionModel fit(DataStream<Tuple2<Integer, LabeledVector>> trainingStream) {
            return new RegressionModel();
        }

        void predict(RegressionModel model, DataStream<Tuple2<Integer, LabeledVector>> testStream) {

        }
    }

    private static class RegressionModel {
        public DenseVector getWeightVector() {
            return null;
        }

        public double getIntercept() {
            return 0.0;
        }
    }
}
