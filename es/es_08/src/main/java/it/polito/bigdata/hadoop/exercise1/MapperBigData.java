package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Exercise 1 - Mapper
 */
class MapperBigData extends Mapper<
        Text, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        FloatWritable> {// Output value type

    protected void map(
            Text key,           // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        String[] array = key.toString().split("-");

        String s = array[0]+"-"+array[1];

            context.write(new Text(s),
                    new FloatWritable(Float.parseFloat((value.toString()))));
    }
}
