package org.clueweb.mapreduce;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.clueweb.app.ESIndexer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * MapReduce Reducer for aggregating MapWritables.
 *
 * @author Janek Bevendorff
 * @version 1
 */
public class CluewebMapReducer extends Reducer<Text, MapWritable, NullWritable, MapWritable>
{
    @Override
    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException
    {
        MapWritable outWritable = getMapTemplate();

        final Text anchorKey = new Text("anchor_texts");
        ArrayList<String> anchorTexts = new ArrayList<>();

        for (MapWritable value : values) {
            // accumulate anchor texts instead of overwriting values
            if (value.keySet().contains(anchorKey)) {
                anchorTexts.add(cleanUpString(value.get(anchorKey).toString()));
                value.remove(anchorKey);
            }
            outWritable.putAll(value);
        }

        outWritable.put(anchorKey, new ArrayWritable(anchorTexts.toArray(new String[anchorTexts.size()])));

        // prettify Text fields by replacing broken Unicode replacement characters with zero-width spaces
        for (Writable k : outWritable.keySet()) {
            if (outWritable.get(k) instanceof Text) {
                outWritable.put(k, new Text(cleanUpString(outWritable.get(k).toString())));
            }
        }

        // only write record if there is content
        if (outWritable.get(new Text("body")).toString().trim().length() > 0) {
            context.write(NullWritable.get(), outWritable);
            context.getCounter(ESIndexer.RecordCounters.GENERATED_DOCS).increment(1);
        } else {
            context.getCounter(ESIndexer.RecordCounters.NO_CONTENT).increment(1);
        }
    }

    /**
     * Generate MapWritable template containing all values with empty values.
     *
     * @return MapWritable template
     */
    private MapWritable getMapTemplate()
    {
        MapWritable templateMap = new MapWritable();
        templateMap.put(new Text("WARC-TREC-ID") ,                new Text(""));
        templateMap.put(new Text("WARC-Warcinfo-ID"),             new Text(""));
        templateMap.put(new Text("WARC-Target-URI"),              new Text(""));
        templateMap.put(new Text("meta_desc"),                    new Text(""));
        templateMap.put(new Text("meta_keywords"),                new Text(""));
        templateMap.put(new Text("anchor_texts"),                 new ArrayWritable(new String[0]));
        templateMap.put(new Text("title"),                        new Text(""));
        templateMap.put(new Text("body"),                         new Text(""));
        templateMap.put(new Text("body_length"),                  new LongWritable(0L));
        //templateMap.put(new Text("raw_html"),                     new Text(""));
        templateMap.put(new Text("page_rank"),                    new FloatWritable(0.0F));
        templateMap.put(new Text("spam_rank"),                    new LongWritable(0L));

        return templateMap;
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
