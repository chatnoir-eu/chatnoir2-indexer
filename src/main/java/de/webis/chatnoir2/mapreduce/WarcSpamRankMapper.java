/*
 * Elasticsearch Indexer for WARC JSON Mapfiles using Hadoop MapReduce.
 * Copyright (C) 2014-2015 Janek Bevendorff <janek.bevendorff@uni-weimar.de>
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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MapReduce Mapper class for Spam rankings.
 *
 * @author Janek Bevendorff
 */
public class WarcSpamRankMapper extends Mapper<LongWritable, Text, Text, MapWritable> implements WarcMapReduceBase
{
    @Override
    public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
    {
        final String[] parts = value.toString().split("\\s+");

        MAPREDUCE_KEY.set(parts[1]);

        OUTPUT_MAP_DOC.clear();
        OUTPUT_MAP_DOC.put(SPAM_RANK_KEY, new LongWritable(Long.valueOf(parts[0])));
        context.write(MAPREDUCE_KEY, OUTPUT_MAP_DOC);
    }
}