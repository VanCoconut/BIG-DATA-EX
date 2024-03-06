package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import com.amazonaws.util.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.mortbay.util.StringUtil;

/**
 * Exercise 1 - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
        FloatWritable,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

       float sum=0;

        // Iterate over the set of values and sum them
        for (FloatWritable value : values) {
            sum+=Float.parseFloat(value.toString());
        }

        context.write(key, new Text(""+sum));
    }
}
