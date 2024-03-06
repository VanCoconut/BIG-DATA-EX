package it.polito.bigdata.spark.exercise30;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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

		JavaRDD<String> sensorRDD = logRDD.map(e -> {
			String[] array = e.split(",");
			return array[0];
		});

		JavaRDD<String> distinctSensorRDD = logRDD.distinct();

		JavaRDD<String> s1RDD = logRDD.filter(e-> e.contains("s1"));
		JavaRDD<String> s2RDD = logRDD.filter(e-> e.contains("s2"));

		JavaRDD<String> sensorTop1RDD = s1RDD.map(e -> {
			String[] array = e.split(",");
			return array[2];
		});
		JavaRDD<String> sensorTop2RDD = s2RDD.map(e -> {
			String[] array = e.split(",");
			return array[2];
		});

		List<String> topS1 = sensorTop1RDD.top(1);
		List<String> topS2 = sensorTop2RDD.top(1);

		System.out.println(topS1+"\n"+topS2);

		JavaRDD<String> s1TopRDD = logRDD.filter(e-> e.contains(topS1.get(0)));
		JavaRDD<String> s2TopRDD = logRDD.filter(e-> e.contains(topS2.get(0)));

		JavaRDD<String> union = s1TopRDD.union(s2TopRDD);

		union.saveAsTextFile(outputPath);
		// Close the Spark context
		sc.close();
	}
}
