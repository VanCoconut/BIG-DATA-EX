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

//		JavaRDD<String> sensorOver50RDD = logRDD.filter( e -> {
//			String[] array = e.split(",");
//
//			if(new Float(array[2])>50) return true;
//			else return false;
//		});


		JavaPairRDD<String,String> sensorRDD = logRDD.mapToPair(e -> {
			String[] array = e.split(",");
			if(new Float(array[2])>50) return new Tuple2<String, String>(array[0],array[1]);
			else return new Tuple2<String, String>(array[0], null);
		});



		JavaPairRDD<String, Iterable<String>> dateRDD = sensorRDD.groupByKey();

		JavaPairRDD<String,Iterable<String>> correctRDD=dateRDD.mapValues((Iterable<String> e)-> {
			List<String> lista = new ArrayList<>();
			for (String date : e) {
				if (date != null) lista.add(date);
			}
			return lista;
		});
		correctRDD.saveAsTextFile(outputPath);
		// Close the Spark context
		sc.close();
	}
}
