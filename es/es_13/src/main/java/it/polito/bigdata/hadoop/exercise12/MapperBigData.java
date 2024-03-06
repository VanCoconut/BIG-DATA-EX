package it.polito.bigdata.hadoop.exercise12;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper
 */
class MapperBigData extends
        Mapper<Text, // Input key type
                Text, // Input value type
                Text, // Output key type
                IntWritable> {// Output value type

    int max;
    String data;

    protected void setup(Context context) {
        // I retrieve the value of the threshold only one time for each mapper
        max = 0;

    }

    protected void map(Text key, // Input key type
                       Text value, // Input value type
                       Context context) throws IOException, InterruptedException {

        String a = key.toString();


        // Filter the reading based on the value of threshold
        if (max < Integer.parseInt(value.toString())) {

            data=key.toString();
            max=Integer.parseInt(value.toString());

        }
    }

    @Override
    protected void cleanup(Mapper<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        context.write(new Text(data),
                new IntWritable(max));
    }
}

