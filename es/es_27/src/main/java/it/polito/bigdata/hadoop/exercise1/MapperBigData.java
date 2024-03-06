package it.polito.bigdata.hadoop.exercise1;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.hash.Hash;


/**
 * Exercise 1 - Mapper
 */
class MapperBigData extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        NullWritable> {// Output value type

    private Map<String, String> rules;

    protected void setup(Context context) throws IOException, InterruptedException {
        String nextLine;
        rules = new HashMap<>();
        // Open the stopword file (that is shared by means of the distributed
        // cache mechanism)
        URI[] urisCachedFiles = context.getCacheFiles();
        // This application has one single single cached file.
        // Its path is stored in urisCachedFiles[0]
        BufferedReader filerules = new BufferedReader(new
                FileReader(new File(urisCachedFiles[0].getPath())));
        // Each line of the file contains one stopword
        // The rules are stored in the rules list
        while ((nextLine = filerules.readLine()) != null) {
            String[] words = nextLine.toString().split("->");
            rules.put(words[0], words[1]);
        }
        filerules.close();
    }


    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // Split each sentence in words. Use whitespace(s) as delimiter
        // (=a space, a tab, a line break, or a form feed)
        // The split method returns an array of strings
        String[] words = value.toString().split(",");
        String result = "";
        // Iterate over the set of words

        // Transform word case
        String cleanedWord = "Gender=" + words[3] + " and YearOfBirth=" + words[4]+" ";

//        System.out.println(cleanedWord);
      // System.out.println(rules.get(cleanedWord));
//        System.out.println(cleanedWord.equalsIgnoreCase(rules.get(" Category#1")));
        for (Map.Entry<String, String> entry : rules.entrySet()) {
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }

        if (rules.get(cleanedWord)!= null) {
            result = value.toString() + "," + rules.get(cleanedWord);
        } else result = value.toString() + ", Unknown" ;

        context.write(new Text(result), NullWritable.get());
    }
}
