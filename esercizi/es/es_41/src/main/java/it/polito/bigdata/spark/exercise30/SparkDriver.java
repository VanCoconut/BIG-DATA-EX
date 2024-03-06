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
		int k;

		inputPath = args[0];
		outputPath = args[1];
		k= new Integer(args[2]);

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

		JavaPairRDD<String,Integer> sensorRDD = sensorOver50RDD.mapToPair(e -> {
			String[] array = e.split(",");
			if(new Float(array[2])>50) return new Tuple2<String, Integer>(array[0],1);
			else return new Tuple2<String, Integer>(array[0],0);
		});



		JavaPairRDD<String, Integer> sumRDD = sensorRDD.reduceByKey((e1,e2)->e1+e2);

		JavaPairRDD<Integer, String> numCriticalValuesSensorRDD = sumRDD.mapToPair((Tuple2<String, Integer> inPair) ->
						new Tuple2<Integer, String>(inPair._2(), inPair._1()));

		JavaPairRDD<Integer, String> orderedRDD = numCriticalValuesSensorRDD.sortByKey(false);


		List<Tuple2<Integer, String>> top= orderedRDD.take(k);


		sc.parallelizePairs(top).saveAsTextFile(outputPath);
		// Close the Spark context
		sc.close();
	}
}