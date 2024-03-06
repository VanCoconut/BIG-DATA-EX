package it.polito.bigdata.hadoop.exercise5;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCount Reducer
 */
class ReducerBigData extends
		Reducer<NullWritable, // Input key type
				DateIncome, // Input value type
				Text, // Output key type
				FloatWritable> { // Output value type

	DateIncome top1;
	DateIncome top2;

	@Override
	protected void setup(Reducer<NullWritable, DateIncome, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
		top1=new DateIncome();
		top1.setIncome(Float.MIN_VALUE);
		top1.setDate("");
		top2=new DateIncome();
		top2.setIncome(Float.MIN_VALUE);
		top2.setDate("");
	}

	@Override
	protected void reduce(NullWritable key, // Input key type
			Iterable<DateIncome> values, // Input value type
			Context context) throws IOException, InterruptedException {

		// Iterate over the set of values and sum them.
		// Count also the number of values
		for (DateIncome value : values) {
			if(value.getIncome()>top1.getIncome()){
				top2.setDate(top1.getDate());
				top2.setIncome(top1.getIncome());
				
				top1.setDate(value.getDate());
				top1.setIncome(value.getIncome());
			} else if (value.getIncome()>top2.getIncome()) {
				top2.setDate(value.getDate());
				top2.setIncome(value.getIncome());
			}
		}

		// Compute average value
		// Emits pair (sensor_id, average)

		context.write(new Text(top1.getDate()), new FloatWritable(top1.getIncome()));
		context.write(new Text(top2.getDate()), new FloatWritable(top2.getIncome()));
	}
}
