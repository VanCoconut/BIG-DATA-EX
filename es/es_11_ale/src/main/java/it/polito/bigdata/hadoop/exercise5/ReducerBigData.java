package it.polito.bigdata.hadoop.exercise5;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCount Reducer
 */
class ReducerBigData extends
		Reducer<Text, // Input key type
				StatisticsWritable, // Input value type
				Text, // Output key type
				Text> { // Output value type


	@Override
	protected void reduce(Text key, // Input key type
			Iterable<StatisticsWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		int count = 0;
		float sum = 0;
		System.out.println("pippo");

		// Iterate over the set of values and sum them.
		// Count also the number of values
		for (StatisticsWritable value : values) {
			sum = sum + value.getSum();

			count = count + value.getCount();
		}

		StatisticsWritable s= new StatisticsWritable(sum, count);
		// Compute average value
		// Emits pair (sensor_id, average)
		System.out.println(key + "/" + s);

		context.write(key, new Text(s.toString()));
	}
}
