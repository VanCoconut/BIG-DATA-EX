package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


/**
 * Exercise 1 - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    NullWritable> {// Output value type

    private MultipleOutputs<Text, NullWritable> mos = null;

    protected void setup(Context context)
    {
        // Create a new MultiOutputs using the context object
        mos = new MultipleOutputs<Text, NullWritable>(context);
    }
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Split each sentence in words. Use whitespace(s) as delimiter
    		// (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
            String[] words = value.toString().split(",");

            // Iterate over the set of words


                String temperature = words[3];
                if(Float.parseFloat(temperature)>30){
                    mos.write("hightemp", new Text(words[3]), NullWritable.get());
                }
                if(Float.parseFloat(temperature)<=30){
                    mos.write("normaltemp", value, NullWritable.get());
                }

    }

    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        // Close the MultiOutputs
        // If you do not close the MultiOutputs object the content of the output
        // files will not be correct
        mos.close();
    }

}
