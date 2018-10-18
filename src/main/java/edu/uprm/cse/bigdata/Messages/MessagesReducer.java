package edu.uprm.cse.bigdata.Messages;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MessagesReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;
        String idList = "";

        for (Text value : values ){
            count++;
            idList += " " + value.toString();
        }
        context.write(key, new Text(count + " " + idList));
    }

}