package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        IntWritable> {// Output value type

    HashMap<String, Integer> map=new HashMap<String, Integer>();
//

//    protected void setup(Context context) {
//        map = new HashMap<String, Integer>();
//    }

    protected void map(
            LongWritable key,           // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        String[] array = value.toString().split("\\s+");
       // context.getCounter(DriverBigData.MY_COUNTERS.TOTAL_RECORDS).increment(1);
        context.write(new Text(""),new IntWritable(1));

    }







}
