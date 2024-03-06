package it.polito.bigdata.hadoop.exercise5;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Average Mapper
 */
class MapperBigData extends Mapper
				<Text, // Input key type
				Text, // Input value type
				NullWritable, // Output key type
				DateIncome> {// Output value type


	DateIncome top1;
	DateIncome top2;

	@Override
	protected void setup(Mapper<Text, Text, NullWritable, DateIncome>.Context context) throws IOException, InterruptedException {
		top1=new DateIncome();
		top1.setIncome(Float.MIN_VALUE);
		top1.setDate("");
		top2=new DateIncome();
		top1.setIncome(Float.MIN_VALUE);
		top1.setDate("");
	}

	protected void map(Text key, // Input key type
					   Text value, // Input value type
					   Context context) throws IOException, InterruptedException {

		String date= key.toString();
		float income=Float.parseFloat(value.toString());

		//System.out.println("locali map"+date+"/"+income);

		if(income>top1.getIncome()){
			top2.setDate(top1.getDate());
			top2.setIncome(top1.getIncome());
			top1.setIncome(income);
			top1.setDate(date);
		}else if(income>top2.getIncome()){
			top2.setIncome(income);
			top2.setDate(date);
		}

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println(top1.getDate()+"/"+top1.getIncome());
			System.out.println(top2.getDate()+"/"+top2.getIncome());
			context.write(NullWritable.get(), top1);
			context.write(NullWritable.get(), top2);
	}
}
