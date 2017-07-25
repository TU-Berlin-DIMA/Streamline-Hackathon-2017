package de.dfki.streamline.hackathon;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.common.LabeledVector;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author behrouz
 */
public class StreamlineHybridLinearRegression {
    static int NUMBER_OF_FEATURES = 100;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer, LabeledVector>> histDataSet = env.readTextFile("hdfs://mycluster/hist.dataset").
                map(new VectorParser());

        String trainingHost = "streamline-hackathon.de/training-stream";
        DataStream<Tuple2<Integer, LabeledVector>> trainingStream = createStreamSource(trainingHost, 8080, env)
                .map(new VectorParser());

        String testHost = "streamline-hackathon.de/test-stream";
        DataStream<Tuple2<Integer, LabeledVector>> testStream = createStreamSource(testHost, 8080, env)
                .map(new VectorParser());

        HybridLinearRegression regressor = new HybridLinearRegression()
                .withInitWeights(DenseVector.zeros(NUMBER_OF_FEATURES), 0.0)
                .withNumIterations(100);

        SideInput<LabeledVector> historicalDataHandle = env.newForwardedSideInput(histDataSet);

        RegressionModel model = regressor
                .setBatchDataSet(historicalDataHandle)
                .fit(trainingStream);

        regressor.predict(model, testStream);

        env.execute();
    }

    private static DataStream<String> createStreamSource(String host, int i, StreamExecutionEnvironment env) {
        return null;
    }


    private static class StreamingSource implements org.apache.flink.streaming.api.functions.source.SourceFunction<String> {

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {

        }

        @Override
        public void cancel() {

        }
    }

    private static class TestStreamingSource implements org.apache.flink.streaming.api.functions.source.SourceFunction<String> {

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {

        }

        @Override
        public void cancel() {

        }
    }


    private static class VectorParser implements MapFunction<String, Tuple2<Integer, LabeledVector>> {

        @Override
        public Tuple2<Integer, LabeledVector> map(String value) throws Exception {
            return new Tuple2<>(0, new LabeledVector(0.0, DenseVector.zeros(NUMBER_OF_FEATURES)));
        }
    }

    private static class HybridLinearRegression {

        HybridLinearRegression withInitWeights(DenseVector zeros, double v) {
            return this;
        }

        HybridLinearRegression withNumIterations(int i) {
            return this;
        }

        HybridLinearRegression setBatchDataSet(SideInput<LabeledVector> batchDataSet) {
            return this;
        }

        RegressionModel fit(DataStream<Tuple2<Integer, LabeledVector>> trainingStream) {
            return new RegressionModel();
        }

        void predict(RegressionModel model, DataStream<Tuple2<Integer, LabeledVector>> testStream) {

        }
    }


    private class SideInput<T> {
    }

    private static class RegressionModel {
    }
}
