package it.polito.bigdata.spark.exercise30;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

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

		JavaPairRDD<String,Float> sensorRDD = logRDD.mapToPair(e -> {
			String[] array = e.split(",");
			return new Tuple2<String, Float>(array[0],new Float(array[2]));
		});

		JavaPairRDD<String,Float> sensorTopRDD = sensorRDD.reduceByKey((a,b)-> {
			if (a<b) return b;
			else return a;
		});

		sensorTopRDD.saveAsTextFile(outputPath);
		// Close the Spark context
		sc.close();
	}
}
