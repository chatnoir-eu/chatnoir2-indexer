package org.clueweb.mapreduce;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

/**
 * MapReduce Reducer for aggregating MapWritables.
 *
 * @author Janek Bevendorff
 * @version 1
 */
public class CluewebMapReducer extends Reducer<Text, MapWritable, NullWritable, MapWritable> implements ClueWebMapReduceBase
{
    protected static Logger logger;
    protected static Counter generatedCounter;
    protected static Counter emptyCounter;

    protected static final ArrayList<String> anchorTextsList = new ArrayList<>();

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
        super.setup(context);

        logger = Logger.getLogger(getClass());

        generatedCounter = context.getCounter(RecordCounters.GENERATED_DOCS);
        emptyCounter     = context.getCounter(RecordCounters.NO_CONTENT);
    }

    @Override
    public void reduce(final Text key, final Iterable<MapWritable> values, final Context context) throws IOException, InterruptedException
    {
        resetOutputMapWritable();
        anchorTextsList.clear();

        for (final MapWritable value : values) {
            // accumulate anchor texts instead of overwriting values
            if (value.keySet().contains(ClueWebMapReduceBase.anchorTextKey)) {
                anchorTextsList.add(cleanUpString(value.get(ClueWebMapReduceBase.anchorTextKey).toString()));
                value.remove(ClueWebMapReduceBase.anchorTextKey);
            }

            // add all remaining keys to output map
            outputDoc.putAll(value);
        }

        // append accumulated anchor texts
        outputDoc.put(ClueWebMapReduceBase.anchorTextKey, new ArrayWritable(anchorTextsList.toArray(new String[anchorTextsList.size()])));

        // prettify Text fields by replacing broken Unicode replacement characters with zero-width spaces
        for (Writable k : outputDoc.keySet()) {
            if (outputDoc.get(k) instanceof Text) {
                outputDoc.put(k, new Text(cleanUpString(outputDoc.get(k).toString())));
            }
        }

        // only write record if there is content
        if (outputDoc.get(ClueWebMapReduceBase.bodyKey).toString().trim().length() > 0) {
            context.write(NullWritable.get(), outputDoc);
            generatedCounter.increment(1);
        } else {
            logger.warn(String.format("Document %s skipped, no content", key.toString()));
            emptyCounter.increment(1);
        }
    }

    /**
     * Reset output MapWritable with empty values.
     */
    private void resetOutputMapWritable()
    {
        outputDoc.put(ClueWebMapReduceBase.warcTrecIdKey,    emptyText);
        outputDoc.put(ClueWebMapReduceBase.warcInfoIdKey,    emptyText);
        outputDoc.put(ClueWebMapReduceBase.warcTargetUriKey, emptyText);
        outputDoc.put(ClueWebMapReduceBase.metaDescKey,      emptyText);
        outputDoc.put(ClueWebMapReduceBase.metaKeywordsKey,  emptyText);
        outputDoc.put(ClueWebMapReduceBase.anchorTextKey,    emptyArrayWritable);
        outputDoc.put(ClueWebMapReduceBase.titleKey,         emptyText);
        outputDoc.put(ClueWebMapReduceBase.bodyKey,          emptyText);
        outputDoc.put(ClueWebMapReduceBase.bodyLengthKey,    emptyLongWritable);
        outputDoc.put(ClueWebMapReduceBase.pageRankKey,      emptyFloatWritable);
        outputDoc.put(ClueWebMapReduceBase.spamRankKey,      emptyLongWritable);
    }

    /**
     * Clean up Strings by replacing broken Unicode replacement characters with zero-width spaces.
     *
     * @param str the String to clean
     * @return cleaned String
     */
    private String cleanUpString(final String str)
    {
        return str.replaceAll("\ufffd", "\u200b");
    }
}
