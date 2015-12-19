package org.clueweb.mapreduce;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MapReduce Mapper class for page ranks.
 *
 * @author Janek Bevendorff
 * @version 1
 */
public class WarcPageRankMapper extends Mapper<LongWritable, Text, Text, MapWritable> implements WarcMapReduceBase
{
    protected static final Text WARC_TREC_ID = new Text();
    protected static final FloatWritable PAGE_RANK_VALUE = new FloatWritable();

    public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
    {
        final String[] parts = value.toString().split("\\s+");

        WARC_TREC_ID.set(parts[0]);
        PAGE_RANK_VALUE.set(Float.valueOf(parts[1]));
        OUTPUT_DOC.put(PAGE_RANK_KEY, PAGE_RANK_VALUE);
        context.write(WARC_TREC_ID, OUTPUT_DOC);
    }
}