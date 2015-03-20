package org.clueweb.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MapReduce Mapper class for ClueWeb Spam rankings.
 *
 * @author Janek Bevendorff
 * @version 1
 */
public class ClueWebSpamRankMapper extends Mapper<LongWritable, Text, Text, MapWritable>
{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        MapWritable doc = new MapWritable();
        String[] parts  = value.toString().split(" ");

        doc.put(new Text("spam_rank"), new LongWritable(Long.parseLong(parts[0])));
        context.write(new Text(parts[1]), doc);
    }
}