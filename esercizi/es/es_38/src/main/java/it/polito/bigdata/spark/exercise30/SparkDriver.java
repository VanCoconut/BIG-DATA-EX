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

		JavaPairRDD<String,Float> sensorRDD = logRDD.mapToPair(e -> {
			String[] array = e.split(",");
			return new Tuple2<String, Float>(array[0],new Float(array[2]));
		});

//		JavaPairRDD<String,Float> sensorTopRDD = sensorRDD.reduceByKey((a,b)-> {
//			if (a<b) return b;
//			else return a;
//		});
		JavaPairRDD<String,Float> sensorOver50RDD = sensorRDD.filter(a-> a._2>50);
		System.out.println(sensorOver50RDD.countByKey());
		Map<String,Long> map= sensorOver50RDD.countByKey();
		ArrayList<Tuple2<String, Integer>> mapOver50 =
				new ArrayList<Tuple2<String, Integer>>();


		for (Map.Entry<String, Long> entry : map.entrySet()) {
			if (entry.getValue()>=2) {
				System.out.println(entry.getValue());
				Tuple2<String, Integer> localPair;
				localPair = new Tuple2<String, Integer>(entry.getKey(), Math.toIntExact(entry.getValue()));
				mapOver50.add(localPair) ;
				System.out.println(localPair);
			}
		}

		mapOver50.forEach(System.out::println);

		JavaPairRDD<String, Integer> output = sc.parallelizePairs(mapOver50);


		output.saveAsTextFile(outputPath);
		// Close the Spark context
		sc.close();
	}
}
