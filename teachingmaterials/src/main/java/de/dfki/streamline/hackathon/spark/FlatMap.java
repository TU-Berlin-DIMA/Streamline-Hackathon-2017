package de.dfki.streamline.hackathon.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.*;

/**
 * @author behrouz
 */
public class FlatMap {
    public static void main(String[] args) throws InterruptedException {
        // Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("FlatMap");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

        ssc.sparkContext().setLogLevel("ERROR");
        List<Integer> integers = Arrays.asList(1, 2, 3, 4);

        //Create rdd
        JavaRDD<Integer> rdd = ssc.sparkContext().parallelize(integers);
        Queue<JavaRDD<Integer>> queue = new LinkedList<>();
        queue.add(rdd);
        //Create dstream from a queue of rdds
        JavaDStream<Integer> integers2 = ssc.queueStream(queue);

        // Flat Map = Takes one element and produces 0 or more elements
        JavaDStream<Integer> doubleIntegers = integers2.flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public Iterator<Integer> call(Integer value) throws Exception {
                return Arrays.asList(value * 2).iterator();
            }
        });

        doubleIntegers.print();
        // 2, 4, 6, 8

        ssc.start();              // Start the computation
        ssc.awaitTermination();   // Wait for the computation to terminate

    }
}
