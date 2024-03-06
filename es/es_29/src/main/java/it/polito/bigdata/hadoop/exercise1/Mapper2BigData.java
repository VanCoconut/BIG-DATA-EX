package it.polito.bigdata.hadoop.exercise1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * Exercise 1 - Mapper
 */
class Mapper2BigData extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        Text> {// Output value type


    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split(",");

        String s = words[3]+","+words[4];

        context.write(new Text(words[0]), new Text(s));
    }
}
