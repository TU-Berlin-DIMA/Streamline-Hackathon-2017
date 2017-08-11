package de.dfki.streamline.hackathon.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * @author behrouz
 */
public class Reduce {
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

       
        JavaPairDStream<String, Integer> tuples = integers2.mapToPair(new PairFunction<Integer, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Integer value) throws Exception {
                if (value % 2 == 0) {
                    return new Tuple2<>("even", value);
                } else {
                    return new Tuple2<>("odd", value);
                }
            }
        });

        JavaPairDStream<String, Integer> sums = tuples.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        sums.print();
        // ("even", 6)
        // ("odd", 4)

        ssc.start();              // Start the computation
        ssc.awaitTermination();   // Wait for the computation to terminate

    }

}
