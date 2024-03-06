package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import com.amazonaws.util.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.mortbay.util.StringUtil;

/**
 * Exercise 1 - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                IntWritable,    // Input value type
                Text,           // Output key type
                LongWritable> {  // Output value type

    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    int c=0;
        for (IntWritable i: values) {
            c+=i.get();
        }
        //Counter c= context.getCounter(DriverBigData.MY_COUNTERS.TOTAL_RECORDS);
        context.write(new Text("Number of String"), new LongWritable(c));
    }
}
