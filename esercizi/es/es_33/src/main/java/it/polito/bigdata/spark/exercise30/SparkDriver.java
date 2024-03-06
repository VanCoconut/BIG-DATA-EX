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

		JavaRDD<String> logRDD = sc.textFile(inputPath);

		JavaRDD<Float> valuesRDD = logRDD.map( e -> {
			String[] array = e.split(",");
			return Float.parseFloat(array[2]);
		});
		List<Float> top = valuesRDD.top(3);


		System.out.println(top);
		// Close the Spark context
		sc.close();
	}
}
