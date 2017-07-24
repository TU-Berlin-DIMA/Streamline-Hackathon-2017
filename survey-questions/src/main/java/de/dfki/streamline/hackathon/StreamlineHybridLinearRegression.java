package de.dfki.streamline.hackathon;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.ml.common.LabeledVector;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

/**
 * @author behrouz
 */
public class StreamlineHybridLinearRegression {
    static int NUMBER_OF_FEATURES = 100;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<LabeledVector> histDataSet = env.readTextFile("hdfs://mycluster/hist.dataset").
                map(new VectorParser());

        AllWindowedStream<LabeledVector, TimeWindow> streamingTrainingSet = env.addSource(new StreamingSource())
                .map(new VectorParser())
                .assignTimestampsAndWatermarks(new TimeStampAndWaterMarkAssigner())
                .timeWindowAll(Time.of(500, TimeUnit.MILLISECONDS));

        AllWindowedStream<LabeledVector, TimeWindow> testStream = env.addSource(new TestStreamingSource())
                .map(new VectorParser())
                .assignTimestampsAndWatermarks(new TimeStampAndWaterMarkAssigner())
                .timeWindowAll(Time.of(500, TimeUnit.MILLISECONDS));

        StreamingLinearRegressionSGD regressor = new StreamingLinearRegressionSGD()
                .withInitWeights(DenseVector.zeros(NUMBER_OF_FEATURES), 0.0)
                .withNumIterations(100);

        SideInput<LabeledVector> histDsHandle = env.newForwardedSideInput(histDataSet);

        RegressionModel model = regressor.fit(streamingTrainingSet, histDsHandle);

        regressor.predict(model, testStream);

        env.execute();

    }

    // Everything below is dummy code to make the code compile
    private static class TimeStampAndWaterMarkAssigner implements
            org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks<LabeledVector> {
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return null;
        }

        @Override
        public long extractTimestamp(LabeledVector o, long l) {
            return 0;
        }
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


    private static class VectorParser implements MapFunction<String, LabeledVector> {

        @Override
        public LabeledVector map(String value) throws Exception {
            return new LabeledVector(0.0, DenseVector.zeros(NUMBER_OF_FEATURES));
        }
    }

    private static class StreamingLinearRegressionSGD {
        StreamingLinearRegressionSGD withInitWeights(DenseVector zeros, double v) {
            return this;
        }

        StreamingLinearRegressionSGD withNumIterations(int i) {
            return this;
        }

        RegressionModel fit(AllWindowedStream<LabeledVector, TimeWindow> streamingTrainingSet, SideInput<LabeledVector> histDsHandle) {
            return new RegressionModel();
        }

        void predict(RegressionModel model, AllWindowedStream<LabeledVector, TimeWindow> testStream) {
        }
    }


    private class SideInput<T> {
    }

    private static class RegressionModel {
    }
}
