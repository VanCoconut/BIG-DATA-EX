package it.polito.bigdata.spark.exercise30;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #30").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> logRDD = sc.textFile(inputPath).cache();

		JavaRDD<String> sensorOver50RDD = logRDD.filter( e -> {
			String[] array = e.split(",");

			if(new Float(array[2])>50) return true;
			else return false;
		});

		System.out.println(sensorOver50RDD.collect());
		//JavaRDD<String> sensorOver50RDD = logRDD.filter(a->a>50);

		JavaPairRDD<String,String> sensorRDD = sensorOver50RDD.mapToPair(e -> {
			String[] array = e.split(",");
			return new Tuple2<String, String>(array[0],array[1]);
		});

		JavaPairRDD<String, Iterable<String>> dateRDD = sensorRDD.groupByKey();


		dateRDD.saveAsTextFile(outputPath);
		// Close the Spark context
		sc.close();
	}
}
