package org.clueweb.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MapReduce Mapper class for Spam rankings.
 *
 * @author Janek Bevendorff
 * @version 1
 */
public class WarcSpamRankMapper extends Mapper<LongWritable, Text, Text, MapWritable> implements WarcMapReduceBase
{
    protected static final Text WARC_TREC_ID = new Text();
    protected static final LongWritable SPAM_RANK_VALUE = new LongWritable();

    public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
    {
        final String[] parts = value.toString().split("\\s+");
        WARC_TREC_ID.set(parts[1]);
        SPAM_RANK_VALUE.set(Long.valueOf(parts[0]));

        OUTPUT_DOC.put(SPAM_RANK_KEY, SPAM_RANK_VALUE);
        context.write(WARC_TREC_ID, OUTPUT_DOC);
    }
}