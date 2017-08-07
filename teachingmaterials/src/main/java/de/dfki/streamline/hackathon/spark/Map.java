package de.dfki.streamline.hackathon.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * @author behrouz
 */
public class Map {
    public static void main(String[] args) throws InterruptedException {
        // Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Map");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

        ssc.sparkContext().setLogLevel("ERROR");
        List<Integer> integers = Arrays.asList(1, 2, 3, 4);

        //Create rdd
        JavaRDD<Integer> rdd = ssc.sparkContext().parallelize(integers);
        Queue<JavaRDD<Integer>> queue = new LinkedList<>();
        queue.add(rdd);
        //Create dstream from a queue of rdds
        JavaDStream<Integer> integers2 = ssc.queueStream(queue);

        // Regular Map = Takes one element and produces one element
        JavaDStream<Integer> doubleIntegers = integers2.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer value) throws Exception {
                return value * 2;
            }
        });

        doubleIntegers.print();
        // 2, 4, 6, 8

        ssc.start();              // Start the computation
        ssc.awaitTermination();   // Wait for the computation to terminate

    }
}
