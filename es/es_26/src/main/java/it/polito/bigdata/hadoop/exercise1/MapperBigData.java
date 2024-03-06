package it.polito.bigdata.hadoop.exercise1;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 1 - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    NullWritable> {// Output value type

    private ArrayList<String> stopWords;

    protected void setup(Context context) throws IOException, InterruptedException
    {
        String nextLine;
        stopWords=new ArrayList<String>();
        // Open the stopword file (that is shared by means of the distributed
        // cache mechanism)
        URI[] urisCachedFiles = context.getCacheFiles();
        // This application has one single single cached file.
        // Its path is stored in urisCachedFiles[0]
        BufferedReader fileStopWords = new BufferedReader(new
                FileReader(new File(urisCachedFiles[0].getPath())));
        // Each line of the file contains one stopword
        // The stopwords are stored in the stopWords list
        while ((nextLine = fileStopWords.readLine()) != null) {
            stopWords.add(nextLine);
        }
        fileStopWords.close();
    }


    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Split each sentence in words. Use whitespace(s) as delimiter
    		// (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
            String[] words = value.toString().split("\\s+");
            String result="";
            // Iterate over the set of words
            for(String word : words) {
            	// Transform word case
                String cleanedWord = word.toLowerCase();

                if(stopWords.contains(cleanedWord)) continue;
                result=result+" "+cleanedWord;

            }
        context.write(new Text(result), NullWritable.get());
    }
}
