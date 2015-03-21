package org.clueweb.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        Pattern r = Pattern.compile("(clueweb\\d{2}-\\w{2}\\d{4}-\\d{2}-\\d{5})\\s+(.*)");
        Matcher m = r.matcher(strValue);

        if (m.matches() && null != m.group(1) && null != m.group(2)) {
            String warcId     = m.group(1);
            String anchorText = m.group(2);
            if (MAX_LENGTH < anchorText.length()) {
                anchorText = anchorText.substring(0, MAX_LENGTH);
            }

            doc.put(new Text("anchor_texts"), new Text(anchorText));
            context.write(new Text(warcId), doc);
        }
    }
}