package de.dfki.streamline.hackathon;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.common.LabeledVector;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @author behrouz
 */
public class StreamlineHybridLinearRegression {
    static int NUMBER_OF_FEATURES = 100;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer, LabeledVector>> histDataSet = env.readTextFile("hdfs://mycluster/hist.dataset")
                .map(new VectorParser());

        String trainingHost = "streamline-hackathon.de/training-stream";
        DataStream<Tuple2<Integer, LabeledVector>> trainingStream = createStreamSource(trainingHost, 8080, env)
                .map(new VectorParser());


        HybridLinearRegression regressor = new HybridLinearRegression()
                .withInitWeights(DenseVector.zeros(NUMBER_OF_FEATURES), 0.0)
                .withNumIterations(100);

        SideInput<LabeledVector> historicalDataHandle = env.newForwardedSideInput(histDataSet);

        regressor.setBatchDataSet(historicalDataHandle)
                .prequentialTrain(trainingStream);

        writeToKafka(regressor, TimeUnit.MINUTES, 5);

        env.execute();
    }

    private static void writeToKafka(HybridLinearRegression streamRegressor, TimeUnit timeUnit, int interval) {

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

    static class HybridLinearRegression {

        HybridLinearRegression withInitWeights(DenseVector zeros, double v) {
            return this;
        }

        HybridLinearRegression withNumIterations(int i) {
            return this;
        }

        HybridLinearRegression setBatchDataSet(SideInput<LabeledVector> batchDataSet) {
            return this;
        }

        DataStream<RegressionModel> prequentialTrain(DataStream<Tuple2<Integer, LabeledVector>> trainingStream) {
            return null;
        }
    }

    private class SideInput<T> {
    }

    private static class RegressionModel {
    }
}
