package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import com.amazonaws.util.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.mortbay.util.StringUtil;

/**
 * Exercise 1 - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                NullWritable,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        String occurrences = "";

        // Iterate over the set of values and sum them
        for (Text value : values) {
            occurrences = occurrences + value.toString()+"\n";
        }



        context.write(NullWritable.get(), new Text(occurrences));
    }
}
