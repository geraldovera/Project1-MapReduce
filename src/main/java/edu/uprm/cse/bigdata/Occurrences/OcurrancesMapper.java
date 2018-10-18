package edu.uprm.cse.bigdata.Occurrences;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;
import java.util.ArrayList;

public class OcurrancesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String tweet = value.toString();

        ArrayList<String> keyWords = new ArrayList<String>();
        keyWords.add("TRUMP");
        keyWords.add("FLU");
        keyWords.add("ZIKA");
        keyWords.add("DIARRHEA");
        keyWords.add("EBOLA");
        keyWords.add("HEADACHE");
        keyWords.add("MEASLES");

        try {
            Status status = TwitterObjectFactory.createStatus(tweet);
            String text = status.getText().toUpperCase();

            for (int i =0; i<keyWords.size();i++){
                if(text.contains(keyWords.get(i))){
                    context.write(new Text(keyWords.get(i)), new IntWritable(1));
                }
            }
        }
        catch(TwitterException e){
            e.printStackTrace();
        }
    }
}
