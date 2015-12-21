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

import de.webis.chatnoir2.util.LangDetector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Mapper class for link anchor texts.
 *
 * @author Janek Bevendorff
 */
public class WarcAnchorMapper extends Mapper<LongWritable, Text, Text, MapWritable> implements WarcMapReduceBase
{
    /**
     * Cut anchor texts after MAX_LENGTH characters.
     */
    public static final int MAX_LENGTH = 400;

    protected static Pattern REGEX;

    protected static LangDetector LANGUAGE_DETECTOR = null;

    @Override
    protected void setup(final Context context) throws IOException
    {
        REGEX = Pattern.compile("(clueweb\\d{2}-\\w{2}\\d{4}-\\d{2}-\\d{5})\\s+(.*)");

        if (null == LANGUAGE_DETECTOR) {
            LANGUAGE_DETECTOR = new LangDetector();
        }
    }

    @Override
    public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
    {
        final String strValue = value.toString();
        final Matcher m = REGEX.matcher(strValue);

        if (m.matches() && null != m.group(1) && null != m.group(2)) {
            final String recordId = m.group(1);
            String anchorValue    = m.group(2);
            if (MAX_LENGTH < anchorValue.length()) {
                anchorValue = anchorValue.substring(0, MAX_LENGTH);
            }

            // language detection
            String lang;
            try {
                lang = LANGUAGE_DETECTOR.detect(anchorValue);
            } catch (IOException e) {
                lang = "en";
                LOG.warn("Language detection for document " + key + "failed, falling back to en");
            }

            MAPREDUCE_KEY.set(recordId);
            ANCHOR_TEXT_VALUE.set(anchorValue);

            OUTPUT_MAP_DOC.clear();
            OUTPUT_MAP_DOC.put(new Text(ANCHOR_TEXTS_BASE_KEY + lang), ANCHOR_TEXT_VALUE);
            context.write(MAPREDUCE_KEY, OUTPUT_MAP_DOC);
        }
    }
}