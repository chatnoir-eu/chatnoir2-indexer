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

import de.webis.chatnoir2.indexer.util.LangDetector;
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
            lang = LANGUAGE_DETECTOR.detect(anchorValue);
            if (lang.isEmpty()) {
                lang = "unknown";
                LOG.warn("Language detection of anchor text for document " + key + " failed");
            }

            MAPREDUCE_KEY.set(recordId);

            OUTPUT_MAP.clear();
            ANCHOR_TEXTS_VALUE.set(anchorValue);
            OUTPUT_MAP.put(new Text(ANCHOR_TEXTS_KEY_PREFIX + lang), ANCHOR_TEXTS_VALUE);
            context.write(MAPREDUCE_KEY, OUTPUT_MAP);
        }
    }
}