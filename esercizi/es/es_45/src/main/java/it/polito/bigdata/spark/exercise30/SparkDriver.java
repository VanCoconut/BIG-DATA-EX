package it.polito.bigdata.spark.exercise30;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

public class SparkDriver {

    public static void main(String[] args) {

        String inputPath;
        String inputPath2;
        String inputPath3;
        String outputPath;
        float threshold;

        inputPath = args[0];
        inputPath2 = args[1];
        inputPath3 = args[2];
        outputPath = args[3];
        threshold = new Float(args[4]);

        // Create a configuration object and set the name of the application
        SparkConf conf = new SparkConf().setAppName("Spark Exercise #30").setMaster("local");

        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> moviesRDD = sc.textFile(inputPath);
        JavaRDD<String> preferencesRDD = sc.textFile(inputPath2);
        JavaRDD<String> watchedmoviesRDD = sc.textFile(inputPath3);

        JavaPairRDD<String, String> preferencesMapRDD = preferencesRDD.mapToPair(e -> {

            String[] array = e.split(",");
            return new Tuple2<>(array[0], array[1]);
        });

        JavaPairRDD<String, String> genreMoviesRDD = moviesRDD.mapToPair(e -> {
            String[] array = e.split(",");
            return new Tuple2<>(array[0], array[2]);
        });

        JavaPairRDD<String, String> moviesWatchedRDD = watchedmoviesRDD.mapToPair(e -> {
            String[] array = e.split(",");
            return new Tuple2<>(array[1], array[0]);
        });

        JavaPairRDD<String, Tuple2<String, String>> joinPairRDD = moviesWatchedRDD.join(genreMoviesRDD);
        JavaPairRDD<String, String> mRDD = joinPairRDD.mapToPair(e -> new Tuple2<>(e._2._1, e._2._2()));

        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> joinPrefRDD = mRDD.cogroup(preferencesMapRDD);


        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> filterRDD = joinPrefRDD.filter(e -> {
            int totFilms = 0;
            int miStoCazz = 0;
            boolean flag = false;
            for (String film : e._2._1) {
                for (String pref : e._2._2) {
                    if (pref.equalsIgnoreCase(film)) {
                        flag = true;
                        continue;
                    }
                }
                if (!flag) miStoCazz++;
                totFilms++;
                flag = false;
            }
            float percent = (float) miStoCazz / totFilms;
            if (percent > threshold) return true;
            else return false;
        });

        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> mapRDD = filterRDD.filter(e -> {
            int totFilms = 0;
            int miStoCazz = 0;
            boolean flag = false;
            List<String> l = new ArrayList<>();
            for (String film : e._2._1) {
                for (String pref : e._2._2) {
                    if (pref.equalsIgnoreCase(film)) {
                        flag = true;
                        continue;
                    }
                }
                if (!flag) l.add(film);
                flag = false;
                if (l.size() >= 5) return true;
            }
            return false;
        });

        JavaPairRDD<String, Iterable<String>> map1RDD = mapRDD.mapValues(e -> {
            int totFilms = 0;
            int miStoCazz = 0;
            boolean flag = false;
            List<String> l = new ArrayList<>();
            for (String film : e._1) {
                for (String pref : e._2) {
                    if (pref.equalsIgnoreCase(film)) {
                        flag = true;
                        continue;
                    }
                }
                if (!flag) l.add(film);
                flag = false;
            }
            l=l.stream().distinct().collect(Collectors.toList());
            return l;
        });

        map1RDD.saveAsTextFile(outputPath);
        // Close the Spark context
        sc.close();
    }
}