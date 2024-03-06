package it.polito.bigdata.hadoop.exercise1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Exercise 1 - Reducer
 */
class ReducerBigDataStep2 extends Reducer<
                Text,           // Input key type
                FloatWritable,    // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        float sum=0;
        int count=0;
        // Iterate over the set of values and sum them
        for (FloatWritable value : values) {
            sum+=value.get();
            count++;
        }

        context.write(key, new FloatWritable(sum/count));
    }
}
