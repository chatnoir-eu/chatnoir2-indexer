/*
 * Elasticsearch Indexer for WARC JSON Mapfiles using Hadoop MapReduce.
 * Copyright (C) 2014-2017 Janek Bevendorff <janek.bevendorff@uni-weimar.de>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package de.webis.chatnoir2.chatnoir2_indexer.mapreduce;

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
