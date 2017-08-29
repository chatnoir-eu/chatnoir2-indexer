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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MapReduce Mapper class for page ranks.
 *
 * @author Janek Bevendorff
 */
public class WarcPageRankMapper extends Mapper<LongWritable, Text, Text, MapWritable> implements WarcMapReduceBase
{
    @Override
    public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
    {
        final String[] parts = value.toString().split("\\s+");

        MAPREDUCE_KEY.set(parts[0]);

        OUTPUT_MAP.clear();
        PAGE_RANK_VALUE.set(Float.valueOf(parts[1]));
        OUTPUT_MAP.put(PAGE_RANK_KEY, PAGE_RANK_VALUE);
        context.write(MAPREDUCE_KEY, OUTPUT_MAP);
    }
}