package org.clueweb.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MapReduce Mapper class for page ranks.
 *
 * @author Janek Bevendorff
 */
public class WarcPageRankMapper extends Mapper<LongWritable, Text, Text, MapWritable> implements WarcMapReduceBase
{
    @Override
    public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
    {
        final String[] parts = value.toString().split("\\s+");

        MAPREDUCE_KEY.set(parts[0]);
        PAGE_RANK_VALUE.set(Double.valueOf(parts[1]));

        OUTPUT_MAP_DOC.clear();
        OUTPUT_MAP_DOC.put(PAGE_RANK_KEY, PAGE_RANK_VALUE);
        context.write(MAPREDUCE_KEY, OUTPUT_MAP_DOC);
    }
}