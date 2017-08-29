/*
 * ChatNoir Indexing Backend.
 * Copyright (C) 2014-2017 Janek Bevendorff, Webis Group
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package de.webis.chatnoir2.indexer.mapreduce;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Reducer for aggregating mapper results and generating JSON docs which are sent to ElasticSearch.
 *
 * @author Janek Bevendorff
 */
public class WarcReducer extends Reducer<Text, MapWritable, NullWritable, MapWritable> implements WarcMapReduceBase
{
    protected static Counter GENERATED_COUNTER;
    protected static Counter EMPTY_COUNTER;
    protected static Counter PARSE_ERROR_COUNTER;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
        super.setup(context);

        PARSE_ERROR_COUNTER = context.getCounter(RecordCounters.SKIPPED_RECORDS_JSON_PARSE_ERROR);
        GENERATED_COUNTER   = context.getCounter(RecordCounters.GENERATED_DOCS);
        EMPTY_COUNTER       = context.getCounter(RecordCounters.NO_CONTENT);
    }

    @Override
    public void reduce(final Text key, final Iterable<MapWritable> values, final Context context) throws IOException, InterruptedException
    {
        OUTPUT_MAP.clear();

        boolean containsContent = false;
        final HashMap<Text, ArrayList<Text>> anchors = new HashMap<>();

        for (final MapWritable value : values) {
            for (final Writable k : value.keySet()) {
                final String keyStr = k.toString();
                final Writable val = value.get(k);

                if (keyStr.startsWith(ANCHOR_TEXTS_KEY_PREFIX)) {
                    if (!anchors.containsKey(k)) {
                        anchors.put((Text) k, new ArrayList<>());
                    }
                    anchors.get(k).add((Text) val);
                } else {
                    containsContent |= (keyStr.startsWith(BODY_KEY_PREFIX) && !val.toString().trim().isEmpty());
                    OUTPUT_MAP.put(k, val);
                }
            }
        }

        // don't continue if there is no content
        if (!containsContent) {
            LOG.warn(String.format("Document %s skipped, no content", key.toString()));
            EMPTY_COUNTER.increment(1);
            return;
        }

        for (Text k: anchors.keySet()) {
            final ArrayWritable anchorsWritable = new ArrayWritable(Text.class);
            anchorsWritable.set(anchors.get(k).toArray(new Text[anchors.get(k).size()]));
            OUTPUT_MAP.put(k, anchorsWritable);
        }

        context.write(NullWritable.get(), OUTPUT_MAP);
        GENERATED_COUNTER.increment(1);
    }
}
