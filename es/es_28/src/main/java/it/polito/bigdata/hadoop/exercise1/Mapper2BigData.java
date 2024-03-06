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

        String s = words[0]+","+words[3];

        System.out.println("sono nel mapper2 s:"+s);

        context.write(new Text(words[1]), new Text(s));
    }
}
