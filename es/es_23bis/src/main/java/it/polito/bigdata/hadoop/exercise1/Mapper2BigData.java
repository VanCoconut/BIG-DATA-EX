package it.polito.bigdata.hadoop.exercise1;

import java.io.BufferedReader;

import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Exercise 1 - Mapper
 */
class Mapper2BigData extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        NullWritable,         // Output key type
        Text> {// Output value type

    private ArrayList<String> inputUsers;
    private ArrayList<String> output;

    protected void setup(Context context) throws IOException, InterruptedException {
        String nextLine;
        System.out.println("siamo nel mapper2");

        inputUsers = new ArrayList<String>();
        output = new ArrayList<String>();
        // Open the stopword file (that is shared by means of the distributed 
        // cache mechanism) 
        URI[] urisCachedFiles = context.getCacheFiles();
        System.out.println(new Path(urisCachedFiles[0].getPath()).getName());

        // This application has one single cached file.
        // Its path is stored in urisCachedFiles[0] 
        BufferedReader fileinputUsers =
                new BufferedReader(new FileReader(new File("C:/Users/vcata/Desktop/Polito/Big Data/HADOOP/esercizi/es/es_23/res/part-r-00000")));


        // Each line of the file contains one stopword
        // The inputUsers are stored in the inputUsers list
        while ((nextLine = fileinputUsers.readLine()) != null) {
            inputUsers.add(nextLine);
        }
        inputUsers.forEach(System.out::println);
        fileinputUsers.close();
        System.out.println("siamo alla fine del buffer");
    }


    protected void map(
            LongWritable key,           // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
        System.out.println("siamo nella funzione map2");
        String[] users = value.toString().split(",");
        Arrays.stream(users).forEach(System.out::println);
        System.out.println("ciao");
        System.out.println(inputUsers.contains(users[0]) + " qui " + inputUsers.contains(users[1]));
        if (inputUsers.contains(users[0]) && !inputUsers.contains(users[1]) && !users[1].equalsIgnoreCase("user2") && !output.contains(users[1])) {
            context.write(NullWritable.get(), new Text(users[1]));
            output.add(users[1]);
        }
        if (inputUsers.contains(users[1]) && !inputUsers.contains(users[0]) && !users[0].equalsIgnoreCase("user2") && !output.contains(users[0])) {
            context.write(NullWritable.get(), new Text(users[0]));
            output.add(users[0]);
        }
        //inputUsers.get(inputUsers.indexOf(users[0])))
    }
}
