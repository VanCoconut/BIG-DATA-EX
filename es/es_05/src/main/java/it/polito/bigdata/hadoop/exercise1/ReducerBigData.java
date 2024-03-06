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
        StatisticsWritable,    // Input value type
                Text,           // Output key type
        StatisticsWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<StatisticsWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        double sum = 0;
        int count=0;
        StatisticsWritable s = new StatisticsWritable();
        // Iterate over the set of values and sum them
        for (StatisticsWritable value : values) {
            sum += value.getSum();
            count+=value.getCount();
        }
        s.setSum(sum);
        s.setCount(count);
        context.write(key, s);
    }
}
