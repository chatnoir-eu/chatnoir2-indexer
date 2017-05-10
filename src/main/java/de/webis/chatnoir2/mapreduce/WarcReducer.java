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

package de.webis.chatnoir2.mapreduce;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

/**
 * Reducer for aggregating mapper results and generating JSON docs which are sent to ElasticSearch.
 *
 * @author Janek Bevendorff
 */
public class WarcReducer extends Reducer<Text, MapWritable, NullWritable, BytesWritable> implements WarcMapReduceBase
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
        try {
            final JSONObject outputJson = new JSONObject();
            boolean containsContent = false;

            for (final MapWritable value : values) {
                for (final Writable k : value.keySet()) {
                    final String keyStr = k.toString();
                    final Writable val = value.get(k);

                    if (keyStr.startsWith(ANCHOR_TEXTS_KEY_PREFIX)) {
                        if (null == outputJson.get(keyStr)) {
                            outputJson.put(keyStr, new JSONArray());
                        }
                        outputJson.getJSONArray(keyStr).put(cleanUpString(val.toString()));
                    } else {
                        if (val instanceof IntWritable) {
                            outputJson.put(keyStr, ((IntWritable) val).get());
                        } else if (val instanceof LongWritable) {
                            outputJson.put(keyStr, ((LongWritable) val).get());
                        } else if (val instanceof FloatWritable) {
                            outputJson.put(keyStr, ((FloatWritable) val).get());
                        } else if (val instanceof DoubleWritable) {
                            outputJson.put(keyStr, ((DoubleWritable) val).get());
                        } else if (val instanceof BooleanWritable) {
                            outputJson.put(keyStr, ((BooleanWritable) val).get());
                        } else {
                            final String valStr = val.toString();
                            containsContent |= (keyStr.startsWith(BODY_KEY_PREFIX) && !valStr.trim().isEmpty());
                            outputJson.put(keyStr, cleanUpString(valStr));
                        }
                    }
                }
            }

            // only write record if there is content
            if (containsContent) {
                final byte[] jsonSerialization = outputJson.toString().getBytes();
                OUTPUT_JSON_DOC.set(jsonSerialization, 0, jsonSerialization.length);
                context.write(NullWritable.get(), OUTPUT_JSON_DOC);
                GENERATED_COUNTER.increment(1);
            } else {
                LOG.warn(String.format("Document %s skipped, no content", key.toString()));
                EMPTY_COUNTER.increment(1);
            }
        } catch (JSONException e) {
            LOG.warn("Document " + key + " skipped due to JSON parsing error: " + e.getMessage());
            PARSE_ERROR_COUNTER.increment(1);
        }
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
