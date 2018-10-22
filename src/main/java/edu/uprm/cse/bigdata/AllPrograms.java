package edu.uprm.cse.bigdata;

import edu.uprm.cse.bigdata.Keywords.Keywords;
import edu.uprm.cse.bigdata.Keywords.KeywordsMapper;
import edu.uprm.cse.bigdata.Keywords.KeywordsReducer;
import edu.uprm.cse.bigdata.Messages.Messages;
import edu.uprm.cse.bigdata.Messages.MessagesMapper;
import edu.uprm.cse.bigdata.Messages.MessagesReducer;
import edu.uprm.cse.bigdata.Occurrences.Occurrences;
import edu.uprm.cse.bigdata.Occurrences.OcurrancesMapper;
import edu.uprm.cse.bigdata.Occurrences.OcurrancesReducer;
import edu.uprm.cse.bigdata.Replies.Replies;
import edu.uprm.cse.bigdata.Replies.RepliesMapper;
import edu.uprm.cse.bigdata.Replies.RepliesReducer;
import edu.uprm.cse.bigdata.ScreenNames.ScreenNames;
import edu.uprm.cse.bigdata.ScreenNames.ScreenNamesMapper;
import edu.uprm.cse.bigdata.ScreenNames.ScreenNamesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AllPrograms {
    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            System.err.println("Usage: CountNamesByState <input path> <output path>");
            System.exit(-1);
        }

        //Occurrences
        Configuration conf1 = new Configuration();
        conf1.set("mapred.textoutputformat.separator", ",");
        Job job1 = Job.getInstance(conf1, "Occurrences");
        job1.setJarByClass(Occurrences.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.setMapperClass(OcurrancesMapper.class);
        job1.setReducerClass(OcurrancesReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.waitForCompletion(true);

        //Keywords
        Configuration conf2 = new Configuration();
        conf2.set("mapred.textoutputformat.separator", ",");
        Job job2 = Job.getInstance(conf2, "Keywords");
        job2.setJarByClass(Keywords.class);

        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.setMapperClass(KeywordsMapper.class);
        job2.setReducerClass(KeywordsReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.waitForCompletion(true);

        //ScreenNames
        Configuration conf3 = new Configuration();
        conf3.set("mapred.textoutputformat.separator", ",");
        Job job3 = Job.getInstance(conf3, "ScreenNames");
        job3.setJarByClass(ScreenNames.class);

        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));

        job3.setMapperClass(ScreenNamesMapper.class);
        job3.setReducerClass(ScreenNamesReducer.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);

        job3.waitForCompletion(true);

        //Replies
        Configuration conf5 = new Configuration();
        conf5.set("mapred.textoutputformat.separator", ",");
        Job job5 = Job.getInstance(conf5, "Replies");
        job5.setJarByClass(Replies.class);

        FileInputFormat.addInputPath(job5, new Path(args[0]));
        FileOutputFormat.setOutputPath(job5, new Path(args[4]));

        job5.setMapperClass(RepliesMapper.class);
        job5.setReducerClass(RepliesReducer.class);

        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        job5.waitForCompletion(true);

        //Messages
        Configuration conf6 = new Configuration();
        conf6.set("mapred.textoutputformat.separator", ",");
        Job job6 = Job.getInstance(conf6, "Messages");
        job6.setJarByClass(Messages.class);

        FileInputFormat.addInputPath(job6, new Path(args[0]));
        FileOutputFormat.setOutputPath(job6, new Path(args[5]));

        job6.setMapperClass(MessagesMapper.class);
        job6.setReducerClass(MessagesReducer.class);

        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(Text.class);

        System.exit(job6.waitForCompletion(true) ? 0 : 1);
    }
}
