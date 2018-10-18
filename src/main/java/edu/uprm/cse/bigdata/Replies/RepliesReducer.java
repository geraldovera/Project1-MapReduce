package edu.uprm.cse.bigdata.Replies;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RepliesReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String idList = "";

        for (Text value : values ){
            idList += " " + value.toString();
        }
        context.write(key, new Text(idList));
    }

}
