package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
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
        Text> {// Output value type

    protected void map(
            Text key,           // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        String[] array = value.toString().split("\\s+");

        for (String s : array) {

            s= s.toLowerCase();
            if (s.equalsIgnoreCase("or") || s.equalsIgnoreCase("and") || s.equalsIgnoreCase("not") ){
                continue;
            }
            context.write(new Text(s),
                    new Text(key));
        }
    }
}
