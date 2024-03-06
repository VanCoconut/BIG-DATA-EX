package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.fs.Stat;
import org.apache.hadoop.io.DoubleWritable;
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
                    StatisticsWritable> {// Output value type
    
    protected void map(
            LongWritable key,           // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] array = value.toString().split(",");
            String sensor_id= array[0];
            double heat= Double.parseDouble(array[2]);

            StatisticsWritable s = new StatisticsWritable();
            s.setSum(heat);
            s.setCount(1);
            context.write(new Text(sensor_id),
                    s);

    }
}
