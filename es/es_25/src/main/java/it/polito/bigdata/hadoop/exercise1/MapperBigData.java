package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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
    Set<String> userList;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        input = context.getConfiguration().get("User");
        userList = new HashSet<>();

    }

    protected void map(
            LongWritable key,           // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        String[] users = value.toString().split(",");

        // Arrays.stream(users).forEach(System.out::println);
        if (users[0].equalsIgnoreCase(input)) context.write(new Text(users[0]), new Text(users[1]));

        if (users[1].equalsIgnoreCase(input)) context.write(new Text(users[1]), new Text(users[0]));
    }

}
