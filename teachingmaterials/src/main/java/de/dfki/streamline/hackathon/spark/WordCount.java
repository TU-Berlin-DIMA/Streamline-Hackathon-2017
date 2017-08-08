package de.dfki.streamline.hackathon.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author behrouz
 */
public class WordCount {
    public static void main(String[] args) throws InterruptedException {
        // Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Socket WordCount Example");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));


        // Split each line into words
        JavaPairDStream<String, Integer> counts = jssc
                // read stream of words from socket
                .socketTextStream("localhost", 9999)
                // split up the lines in the tuples containing: (word, 1)
                .flatMap(new Splitter())
                // transform the tuple into a PairedDStream
                .mapToPair(new ToPair())
                // sum up the second field
                .reduceByKeyAndWindow(new WordCounter(), Durations.minutes(5));

        // Print result in command line
        counts.print();

        // Start the computation
        jssc.start();
        // Wait for the computation to terminate
        jssc.awaitTermination();
    }

    public static class Splitter implements FlatMapFunction<String, String> {
        @Override
        public Iterator<String> call(String value) throws Exception {
            return Arrays.asList(value.split(" ")).iterator();
        }
    }


    public static class WordCounter implements Function2<Integer, Integer, Integer> {
        @Override
        public Integer call(Integer value1, Integer value2) throws Exception {
            return value1 + value2;
        }
    }

    public static class ToPair implements PairFunction<String, String, Integer> {
        @Override
        public Tuple2<String, Integer> call(String value) throws Exception {
            return new Tuple2<>(value, 1);
        }
    }
}
