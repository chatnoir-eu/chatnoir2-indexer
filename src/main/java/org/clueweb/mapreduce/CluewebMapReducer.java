package org.clueweb.mapreduce;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.clueweb.app.ESIndexer;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Set;

/**
 * Reducer for aggregating ClueWeb MapWritables.
 *
 * @author Janek Bevendorff
 * @version 1
 */
public class CluewebMapReducer extends Reducer<Text, MapWritable, NullWritable, MapWritable> implements ClueWebMapReduceBase
{
    protected static final Logger LOG = Logger.getLogger(CluewebMapReducer.class);

    protected static Counter generatedCounter;
    protected static Counter emptyCounter;

    protected static final ArrayList<String> ANCHOR_TEXTS_LIST = new ArrayList<>();
    protected static final Text LANG_VALUE = new Text();

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
        super.setup(context);

        generatedCounter = context.getCounter(RecordCounters.GENERATED_DOCS);
        emptyCounter     = context.getCounter(RecordCounters.NO_CONTENT);
    }

    @Override
    public void reduce(final Text key, final Iterable<MapWritable> values, final Context context) throws IOException, InterruptedException
    {
        resetOutputMapWritable();
        ANCHOR_TEXTS_LIST.clear();

        for (final MapWritable value : values) {
            // accumulate anchor texts instead of overwriting values
            if (value.containsKey(ANCHOR_TEXTS_KEY)) {
                ANCHOR_TEXTS_LIST.add(cleanUpString(value.get(ANCHOR_TEXTS_KEY).toString()));
                value.remove(ANCHOR_TEXTS_KEY);
            }

            // add all remaining keys to output map
            OUTPUT_DOC.putAll(value);
        }

        // append accumulated anchor texts
        OUTPUT_DOC.put(ANCHOR_TEXTS_KEY, new ArrayWritable(ANCHOR_TEXTS_LIST.toArray(new String[ANCHOR_TEXTS_LIST.size()])));

        // prettify Text fields by replacing broken Unicode replacement characters with zero-width spaces
        for (Writable k : OUTPUT_DOC.keySet()) {
            if (OUTPUT_DOC.get(k) instanceof Text) {
                final Text t = ((Text) OUTPUT_DOC.get(k));
                t.set(cleanUpString(t.toString()));
                OUTPUT_DOC.put(k, t);
            }
        }

        // only write record if there is content
        final String content = OUTPUT_DOC.get(BODY_KEY).toString().trim();
        if (!content.isEmpty()) {
            // language detection
            final URL url            = new URL(String.format("http://%s/_langdetect", ESIndexer.getTargetHost()));
            final URLConnection conn = url.openConnection();
            conn.setDoOutput(true);
            final PrintStream ps = new PrintStream(conn.getOutputStream());
            ps.print(content);
            ps.close();

            BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            final StringBuilder strBuilder = new StringBuilder();
            while (null != (line = br.readLine())) {
                strBuilder.append(line);
            }
            br.close();

            final JSONObject json;
            try {
                json = new JSONObject(strBuilder.toString());
                LANG_VALUE.set(json.getJSONArray("languages").getJSONObject(0).getString("language"));
            } catch (JSONException e) {
                LANG_VALUE.set("en");
                LOG.warn(String.format("JSON error: %s%nOriginal JSON string was: %s", e.getMessage(), strBuilder.toString()));
            }
            OUTPUT_DOC.put(LANG_KEY, LANG_VALUE);

            context.write(NullWritable.get(), OUTPUT_DOC);
            generatedCounter.increment(1);
        } else {
            LOG.warn(String.format("Document %s skipped, no content", key.toString()));
            emptyCounter.increment(1);
        }
    }

    /**
     * Reset output MapWritable with empty values.
     */
    private void resetOutputMapWritable()
    {
        OUTPUT_DOC.put(WARC_TREC_ID_KEY,         EMPTY_TEXT);
        OUTPUT_DOC.put(WARC_INFO_ID_KEY,         EMPTY_TEXT);
        OUTPUT_DOC.put(WARC_TARGET_URI_KEY,      EMPTY_TEXT);
        OUTPUT_DOC.put(WARC_TARGET_HOSTNAME_KEY, EMPTY_TEXT);
        OUTPUT_DOC.put(WARC_TARGET_PATH_KEY,     EMPTY_TEXT);
        OUTPUT_DOC.put(WARC_TARGET_QUERY_KEY,    EMPTY_TEXT);
        OUTPUT_DOC.put(LANG_KEY,                 EMPTY_TEXT);
        OUTPUT_DOC.put(META_DESC_KEY,            EMPTY_TEXT);
        OUTPUT_DOC.put(META_KEYWORDS_KEY,        EMPTY_TEXT);
        OUTPUT_DOC.put(ANCHOR_TEXTS_KEY,         EMPTY_ARRAY_WRITABLE);
        OUTPUT_DOC.put(TITLE_KEY,                EMPTY_TEXT);
        OUTPUT_DOC.put(BODY_KEY,                 EMPTY_TEXT);
        OUTPUT_DOC.put(BODY_LENGTH_KEY,          EMPTY_LONG_WRITABLE);
        OUTPUT_DOC.put(PAGE_RANK_KEY,            NEUTRAL_FLOAT_WRITABLE);
        OUTPUT_DOC.put(SPAM_RANK_KEY,            NEUTRAL_LONG_WRITABLE);
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
