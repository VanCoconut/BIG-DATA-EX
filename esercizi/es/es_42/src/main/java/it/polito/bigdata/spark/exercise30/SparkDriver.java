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
		String inputPath2;
		String outputPath;

		inputPath = args[0];
		inputPath2 = args[1];
		outputPath = args[2];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #30").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> answersRDD = sc.textFile(inputPath);
		JavaRDD<String> questionsRDD = sc.textFile(inputPath2);

		JavaPairRDD<String,String> questionsMapRDD = questionsRDD.mapToPair(e->{
			String[] array = e.split(",");
			return new Tuple2<>(array[0],array[2]);
		});
		JavaPairRDD<String,String> answerMapRDD = answersRDD.mapToPair(e->{
			String[] array = e.split(",");
			return new Tuple2<>(array[1],array[3]);
		});

		//soluzione nostrana brutta
		//JavaPairRDD<String,String> unionRDD = questionsMapRDD.union(answerMapRDD);
		//JavaPairRDD<String,Iterable<String>> combineRDD = unionRDD.groupByKey();

		// soluzione prof

		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> questionsAnswersPairRDD =
				questionsMapRDD.cogroup(answerMapRDD);

		questionsAnswersPairRDD.saveAsTextFile(outputPath);
		// Close the Spark context
		sc.close();
	}
}