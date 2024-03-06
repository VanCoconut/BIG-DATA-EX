package it.polito.bigdata.hadoop.exercise1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Exercise 1 - Reducer
 */
class Reducer2BigData extends Reducer<
                NullWritable,           // Input key type
                Text,    // Input value type
               NullWritable,           // Output key type
                Text> {  // Output value type


    @Override
    protected void setup(Reducer<NullWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
        System.out.println("siamo nel reducer2");
    }

    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        String users = "";
        System.out.println("siamo nel reducer2 users:"+users);
        // Iterate over the set of values and sum them
        for (Text value : values) {
            users = users + value.toString()+" ";
        }
        //System.out.println("siamo nel reducer2 users:"+users);
        context.write(NullWritable.get(), new Text(users));
    }
}
