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
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type


    String result;
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        result="";
    }

    protected void map(
            LongWritable key,           // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] users=value.toString().split(",");

            context.write(new Text(users[0]), new Text(users[1]));
            context.write(new Text(users[1]), new Text(users[0]));



    }

}
