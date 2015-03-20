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
public class ClueWebAnchorMapper extends Mapper<LongWritable, Text, Text, MapWritable>
{
    /**
     * Cut anchor texts after MAX_LENGTH characters.
     */
    public static final int MAX_LENGTH = 400;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        MapWritable doc = new MapWritable();
        String strValue = value.toString();
        int splitPos    = strValue.indexOf("\t");
        String warcId   = strValue.substring(0, splitPos);
        String anchorText = strValue.substring(splitPos + 1);
        if (MAX_LENGTH < anchorText.length()) {
            anchorText = anchorText.substring(0, MAX_LENGTH);
        }

        doc.put(new Text("anchor_texts"), new Text(anchorText));
        context.write(new Text(warcId), doc);
    }
}