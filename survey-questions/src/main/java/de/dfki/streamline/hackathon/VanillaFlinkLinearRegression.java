package de.dfki.streamline.hackathon;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.common.LabeledVector;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @author behrouz
 */
public class VanillaFlinkLinearRegression {
    static int NUMBER_OF_FEATURES = 100;

    public static void main(String[] args) throws Exception {
        String inputData = "hdfs://mycluster/hist.dataset";
        String modelPath = "~/mynode/regression-model/";
        trainBatchModel(inputData, modelPath);

        String host = "streamline-hackathon.de/training-stream";
        trainStreamingModel(host, modelPath);

    }

    private static void trainBatchModel(String inputData, String outputPath) throws Exception {
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, LabeledVector>> trainingDataSet = batchEnv.readTextFile(inputData)
                .map(new VectorParser());

        LinearRegressionSGD regressor = new LinearRegressionSGD()
                .withInitWeights(DenseVector.zeros(NUMBER_OF_FEATURES), 0.0)
                .withNumIterations(100);

        regressor.fit(trainingDataSet);

        writeModelToDisk(regressor, outputPath);

        batchEnv.execute();
    }

    private static void trainStreamingModel(String host, String modelPath) throws Exception {
        StreamExecutionEnvironment streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<Tuple2<Integer, LabeledVector>> trainingStream = createStreamSource(host, 8080, streamingEnv)
                .map(new VectorParser());

        LinearRegressionSGD modelBatch = loadInitialModel(modelPath);

        StreamingLinearRegressionSGD streamRegressor = new StreamingLinearRegressionSGD()
                .withInitialModel(modelBatch);

        streamRegressor.prequentialTrain(trainingStream);

        writeToKafka(streamRegressor, TimeUnit.MINUTES, 5);

        streamingEnv.execute();
    }

    private static void writeToKafka(StreamingLinearRegressionSGD streamRegressor, TimeUnit timeUnit, int interval) {

    }

    private static LinearRegressionSGD loadInitialModel(String modelPath) {
        return new LinearRegressionSGD();
    }

    private static void writeModelToDisk(LinearRegressionSGD regressor, String s) {

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

        public void fit(DataSet<Tuple2<Integer, LabeledVector>> trainingDataSet) {

        }
    }

    private static class StreamingLinearRegressionSGD {

        void prequentialTrain(DataStream<Tuple2<Integer, LabeledVector>> trainingStream) {
        }

        void predict(DataStream<Tuple2<Integer, LabeledVector>> testStream) {

        }

        StreamingLinearRegressionSGD withInitialModel(LinearRegressionSGD modelBatch) {
            return null;
        }
    }

}
