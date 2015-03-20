package org.clueweb.mapreduce;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MapReduce Mapper class for ClueWeb page ranks.
 *
 * @author Janek Bevendorff
 * @version 1
 */
public class ClueWebPageRankMapper extends Mapper<LongWritable, Text, Text, MapWritable>
{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        MapWritable doc = new MapWritable();
        String[] parts  = value.toString().split("\t");

        doc.put(new Text("page_rank"), new FloatWritable(Float.parseFloat(parts[1])));
        context.write(new Text(parts[0]), doc);
    }
}