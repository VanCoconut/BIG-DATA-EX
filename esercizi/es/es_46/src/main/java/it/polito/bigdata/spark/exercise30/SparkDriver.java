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
        String outputPath;


        inputPath = args[0];
        outputPath = args[2];

        // Create a configuration object and set the name of the application
        SparkConf conf = new SparkConf().setAppName("Spark Exercise #30").setMaster("local");

        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> logRDD = sc.textFile(inputPath);

        JavaRDD<String> windowRDD = logRDD.filter(e->{

            int count =0;
            boolean flag = true;
            String[] array = e.split(",");

            List<Float> l = new ArrayList<>();

            if (flag) l.add(new Float(array[1]));
            flag= false;

                });


        //map1RDD.saveAsTextFile(outputPath);
        // Close the Spark context
        sc.close();
    }
}