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
				<LongWritable, // Input key type
				Text, // Input value type
				Text, // Output key type
				StatisticsWritable> {// Output value type

	HashMap<String, StatisticsWritable> map;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, StatisticsWritable>.Context context) throws IOException, InterruptedException {
		map= new HashMap<>();
	}

	protected void map(LongWritable key, // Input key type
					   Text value, // Input value type
					   Context context) throws IOException, InterruptedException {

		int counter=0;
		// Split each record by using the field separator
		// fields[0]= first attribute - sensor id
		// fields[1]= second attribute - date
		// fields[2]= third attribute - PM10 value
		String[] fields = value.toString().split(",");
		String sensorId = fields[0];


		if(!map.containsKey(fields[0])){
			map.put(fields[0], new StatisticsWritable(Float.parseFloat(fields[2]), 1));
		}else{
			int i= map.get(fields[0]).getCount() +1;
			float k= map.get(fields[0]).getSum();
			map.put(fields[0], new StatisticsWritable(Float.parseFloat(fields[2])+k, i));
		}

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Map.Entry<String, StatisticsWritable> entry : map.entrySet()) {
			//System.out.println(entry.getKey() + "/" + entry.getValue().getSum()+"/"+entry.getValue().getCount());
			context.write(new Text(entry.getKey()), new StatisticsWritable(entry.getValue().getSum(), entry.getValue().getCount() ));
		}
	}
}
