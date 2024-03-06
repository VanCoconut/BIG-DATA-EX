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

    String input;
    String result;
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        input=context.getConfiguration().get("User");
        result="";
    }

    protected void map(
            LongWritable key,           // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] users=value.toString().split(",");


            if(users[0].equalsIgnoreCase(input)) context.write(new Text(input), new Text(users[1]));

            if(users[1].equalsIgnoreCase(input))  context.write(new Text(input), new Text(users[0]));
    }

}
