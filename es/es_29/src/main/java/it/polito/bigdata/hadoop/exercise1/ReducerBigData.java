package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 1 - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        String s="";
        List<String> lista=new ArrayList<>();
        for(Text value: values){
            lista.add(value.toString());
            if(value.toString().startsWith("M") || value.toString().startsWith("F")){
                s=value.toString();
            }
        }
        if(lista.contains("Commedia") && lista.contains("Adventure")) context.write(new Text(""), new Text(s));
    }
}
