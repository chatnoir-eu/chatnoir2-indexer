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
public class ClueWebSpamRankMapper extends Mapper<LongWritable, Text, Text, MapWritable> implements ClueWebMapReduceBase
{
    protected static final Text warcTrecId            = new Text();
    protected static final LongWritable spamRankValue = new LongWritable();

    public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
    {
        final String[] parts = value.toString().split(" ");
        warcTrecId.set(parts[1]);
        spamRankValue.set(Long.parseLong(parts[0]));

        outputDoc.put(spamRankKey, spamRankValue);
        context.write(warcTrecId, outputDoc);
    }
}