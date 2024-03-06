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

		int treshold;
		String inputPath;
		String inputPath2;
		String outputPath;
		String outputPath2;

		inputPath = args[0];
		inputPath2 = args[1];
		outputPath = args[2];
		outputPath = args[3];
		treshold = new Integer(args[3]);
		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #30").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		//JavaRDD<String> answersRDD = sc.textFile(inputPath);
		JavaRDD<String> logRDD = sc.textFile(inputPath2);
		//JavaRDD<String> logRDD = sc.textFile(inputPath2);
		//JavaRDD<String> RDD = sc.textFile(inputPath2);

		JavaRDD<String> filterlogRDD = logRDD.filter(e-> {
			String[] array= e.split(",");
			if(new Integer(array[5])<treshold){
				return true;
			}
			else return false;
		});

		JavaPairRDD<String, String> filterpairRDD= filterlogRDD.mapToPair(e->{
			String[] array= e.split(",");
			return new Tuple2<>(array[0], array[5]);
		} );
		Map<String, Long> criticalReadings = filterpairRDD.countByKey();

		//criticalReadings.forEach((e1,e2) -> System.out.println(e1+e2));

		JavaPairRDD<String, String> pairRDD= logRDD.mapToPair(e->{
			String[] array= e.split(",");
			return new Tuple2<>(array[0], array[5]);
		} );

		Map<String, Long> totalReadings = pairRDD.countByKey();
		//totalReadings.forEach((e1,e2) -> System.out.println(e1+e2));
		Map<String, Float> resultMap = new HashMap<>();
		List<Tuple2<String, Float>> resultList=new ArrayList<>();


		for (Map.Entry<String, Long> entry : totalReadings.entrySet()) {
			for (Map.Entry<String, Long> entry2 : criticalReadings.entrySet()) {
				if(entry2.getKey().equalsIgnoreCase(entry.getKey())){
					System.out.println(entry2.getKey().equalsIgnoreCase(entry.getKey()));
					float avg = (float) entry2.getValue()/ entry.getValue();
					System.out.println("criticalreadings:"+entry2.getValue()+"totalreadings:"+entry.getValue()+"avg:"+avg);
					if(avg >= 0.1)	{resultMap.put(entry.getKey(), avg*100);
					System.out.println("secondo if");}
				}
			}
		}
		resultMap.forEach((e1,e2) -> System.out.println(e1+e2));
		for (Map.Entry<String, Float> entry : resultMap.entrySet()) {
			resultList.add(new Tuple2<>(entry.getKey(), entry.getValue()));
		}

		JavaPairRDD<String, Float> resultRDD = sc.parallelizePairs(resultList);
		JavaPairRDD<Float, String> invertedRDD = resultRDD.mapToPair(e->new Tuple2<>(e._2, e._1));
		JavaPairRDD<Float, String> sortedinvertedRDD = invertedRDD.sortByKey(false);


		sortedinvertedRDD.saveAsTextFile(outputPath);


		invertedRDD.saveAsTextFile(outputPath);


		// Close the Spark context
		sc.close();
	}
}