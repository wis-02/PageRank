package org.spark;

import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class PageRank {

    private static final Pattern SPACE = Pattern.compile("[ \\t\\x0B\\f\\r]+");

    public static void main(String[] args) throws Exception {

        /*
        if (args.length < 3) {
            System.err.println("Usage: PageRank <inputpath> <outputpath> <number_of_iterations>");
            System.exit(1);
        }*/
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("PageRank").set("spark.driver.memory", "3g");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load input file. In format: 
        //     URL   neighbor URL
        //     URL   neighbor URL
        //     URL   neighbor URL
        //     ...
        JavaRDD<String> lines = sc.textFile("C:\\Users\\USER\\Desktop\\lab-spark\\data\\example3\\input\\web-Google.txt.gz");

        // read edges
        JavaPairRDD<String, String> edges = lines.mapToPair((s) -> {

            // TODO: map each line to a Tuple2<String, String>
            //       use SPACE pattern to split
            String[] urls = SPACE.split(s);
            return new Tuple2<>(urls[0], urls[1]);
        }).distinct();

        // TODO: convert edges to adjacency list
        JavaPairRDD<String, Iterable<String>> adjacencyList = edges.groupByKey();/* missing */;

        // cache
        adjacencyList.cache();

        // TODO: initialize rank to 1.0 for each url in adjacency list 
        JavaPairRDD<String, Double> ranks = adjacencyList.mapValues(urls -> 1.0);/* missing */;

        int iterations = Integer.parseInt("3");

        for (int i = 0; i < iterations; i++) {

            // join rank of each url with neighbors
            JavaPairRDD<String, Tuple2<Iterable<String>, Double>> urlNeighborsAndRank = adjacencyList.join(ranks);

            // compute rank contribution for each url
            JavaPairRDD<String, Double> urlContributions;
            urlContributions = urlNeighborsAndRank.values().flatMapToPair((t) -> {

                Iterable<String> neighbors = t._1();
                Double rank = t._2;
                int neibsize = Iterables.size(neighbors);
                double contributions = rank / neibsize;
                // TODO: count total neighbors using Iterables.size() method
                /* missing */
                List<Tuple2<String, Double>> results = new ArrayList<>();

                // TODO: iterate over all neighbors of a node and output contribution
                //       for that node which is rank / number-of_neighbors
                for (String neighbor : neighbors) {
                    results.add(new Tuple2<String, Double>(neighbor, contributions));
                }

                return results.iterator();

            });

            // TODO: sum all contributions for a url
            JavaPairRDD<String, Double> urlAllContributions = urlContributions.reduceByKey((x, y) -> x + y);
            /* missing */;

            // TODO: recalculate rank from contribution v as 0.15 + v * 0.85 
            ranks = urlAllContributions.mapValues(v -> 0.15 + v * 0.85);
            /* missing */;

        }

        ranks.saveAsTextFile("C:\\Users\\USER\\Desktop\\lab-spark\\data\\example3\\output");

        sc.stop();

    }

}
