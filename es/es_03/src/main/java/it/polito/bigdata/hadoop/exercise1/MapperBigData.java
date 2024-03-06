package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Exercise 1 - Mapper
 */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            Text key,           // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] array = key.toString().split(",");
            String sensor_id= array[0];
            if(Double.parseDouble(value.toString())>50){
                context.write(new Text(sensor_id),
                        new IntWritable(1));
            }
    }
}
