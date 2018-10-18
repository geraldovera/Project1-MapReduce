package edu.uprm.cse.bigdata.ScreenNames;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

public class ScreenNamesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String tweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(tweet);
            String name = status.getUser().getScreenName();

            context.write(new Text(name), new IntWritable(1));
        }
        catch(TwitterException e){
            e.printStackTrace();
        }
    }
}
