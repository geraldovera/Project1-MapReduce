package edu.uprm.cse.bigdata.Replies;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

public class RepliesMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String tweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(tweet);
            Long toStatusID = status.getInReplyToStatusId();
            Long statusID = status.getId();

            context.write(new Text(toStatusID.toString()), new Text(statusID.toString()));
        }
        catch(TwitterException e){
            e.printStackTrace();
        }
    }
}
