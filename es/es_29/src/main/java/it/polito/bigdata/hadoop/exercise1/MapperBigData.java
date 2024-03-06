package it.polito.bigdata.hadoop.exercise1;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.hash.Hash;


/**
 * Exercise 1 - Mapper
 */
class MapperBigData extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        Text> {// Output value type


    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split(",");

        if(words[1].equalsIgnoreCase("Commedia") ||words[1].equalsIgnoreCase("Adventure")) {
            context.write(new Text(words[0]), new Text(words[1]));
        }
    }
}
