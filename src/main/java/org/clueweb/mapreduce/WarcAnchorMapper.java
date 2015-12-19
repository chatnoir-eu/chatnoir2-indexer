package org.clueweb.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * MapReduce Mapper class for Spam rankings.
 *
 * @author Janek Bevendorff
 * @version 1
 */
public class WarcAnchorMapper extends Mapper<LongWritable, Text, Text, MapWritable> implements WarcMapReduceBase
{
    /**
     * Cut anchor texts after MAX_LENGTH characters.
     */
    public static final int MAX_LENGTH = 400;

    protected static final Text WARC_TREC_ID = new Text();
    protected static final Text ANCHOR_TEXT_VALUE = new Text();
    protected static Pattern REGEX;

    @Override
    protected void setup(final Context context)
    {
        REGEX = Pattern.compile("(clueweb\\d{2}-\\w{2}\\d{4}-\\d{2}-\\d{5})\\s+(.*)");
    }

    public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
    {
        final String strValue = value.toString();
        final Matcher m = REGEX.matcher(strValue);

        if (m.matches() && null != m.group(1) && null != m.group(2)) {
            final String tmpId = m.group(1);
            String tmpValue    = m.group(2);
            if (MAX_LENGTH < tmpValue.length()) {
                tmpValue = tmpValue.substring(0, MAX_LENGTH);
            }
            WARC_TREC_ID.set(tmpId);
            ANCHOR_TEXT_VALUE.set(tmpValue);

            OUTPUT_DOC.put(ANCHOR_TEXTS_KEY, ANCHOR_TEXT_VALUE);
            context.write(WARC_TREC_ID, OUTPUT_DOC);
        }
    }
}