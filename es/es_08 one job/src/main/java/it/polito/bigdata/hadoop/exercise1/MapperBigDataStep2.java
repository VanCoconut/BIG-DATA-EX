package it.polito.bigdata.hadoop.exercise1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * Exercise 1 - Mapper
 */
class MapperBigDataStep2 extends Mapper<
        Text, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        FloatWritable> {// Output value type

    protected void map(
            Text key,           // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        String[] array = key.toString().split("-");

        String s = array[0];

            context.write(new Text(s),
                    new FloatWritable(Float.parseFloat(value.toString())));
    }
}
