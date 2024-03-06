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

        for (String s : array) {
            s = s.toLowerCase();
            if (!map.containsKey(s)) {
                map.put(s, 1);
            } else {
                int i = 0;
                //  System.out.println(s+" "+i);
                i = map.get(s) + 1;
                map.remove(s);
                map.put(s, i);
                // System.out.println(s+" "+i);
            }
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
            context.write(new Text(entry.getKey()),
                    new IntWritable(entry.getValue()));
        }
    }


}
