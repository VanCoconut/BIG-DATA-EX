package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import com.amazonaws.util.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.mortbay.util.StringUtil;

/**
 * Exercise 1 - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                DoubleWritable,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<DoubleWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        double max = Double.MIN_VALUE;
        double min = Double.MAX_VALUE;
        // Iterate over the set of values and sum them
        for (DoubleWritable value : values) {
            if(value.get()>max) max=value.get();
            if (value.get()<min) min=value.get();
        }
        String s= "max="+max+"_min="+min;
        context.write(key, new Text(s));
    }
}
