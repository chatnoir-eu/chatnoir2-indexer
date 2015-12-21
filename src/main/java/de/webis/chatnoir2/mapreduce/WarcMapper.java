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

import de.webis.chatnoir2.util.HtmlToPlainText;
import de.webis.chatnoir2.util.LangDetector;
import net.htmlparser.jericho.Source;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Mapper class for WARC JSON records.
 *
 * @author Janek Bevendorff
 */
public class WarcMapper extends Mapper<Text, Text, Text, MapWritable> implements WarcMapReduceBase
{
    protected static Counter RECORDS_COUNTER;
    protected static Counter PARSE_ERROR_COUNTER;
    protected static Counter TOO_LARGE_COUNTER;
    protected static Counter TOO_SMALL_COUNTER;
    protected static Counter TOO_DEEP_COUNTER;
    protected static Counter BINARY_COUNTER;
    protected static Counter LANGDETECT_FAILED_COUNTER;

    protected static final HtmlToPlainText HTML_TO_PLAIN_TEXT = new HtmlToPlainText();

    protected static LangDetector LANGUAGE_DETECTOR = null;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
        super.setup(context);

        RECORDS_COUNTER           = context.getCounter(RecordCounters.RECORDS);
        PARSE_ERROR_COUNTER       = context.getCounter(RecordCounters.SKIPPED_RECORDS_PARSE_ERROR);
        TOO_LARGE_COUNTER         = context.getCounter(RecordCounters.SKIPPED_RECORDS_TOO_LARGE);
        TOO_SMALL_COUNTER         = context.getCounter(RecordCounters.SKIPPED_RECORDS_TOO_SMALL);
        TOO_DEEP_COUNTER          = context.getCounter(RecordCounters.SKIPPED_RECORDS_TOO_DEEP);
        BINARY_COUNTER            = context.getCounter(RecordCounters.SKIPPED_RECORDS_BINARY);
        LANGDETECT_FAILED_COUNTER = context.getCounter(RecordCounters.LANGDETECT_FAILED);

        // disable Jericho log
        net.htmlparser.jericho.Config.LoggerProvider = net.htmlparser.jericho.LoggerProvider.DISABLED;

        if (null == LANGUAGE_DETECTOR) {
            LANGUAGE_DETECTOR = new LangDetector();
        }
    }

    @Override
    public void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException
    {
        RECORDS_COUNTER.increment(1);
        final String valueStr = value.toString();

        LOG.debug("Mapping document " + key);

        // ignore large files
        if (valueStr.getBytes().length > 4 * 1024 * 1024) {
            LOG.warn("Skipped document " + key + " with size " + valueStr.getBytes().length + "bytes (too large)");
            TOO_LARGE_COUNTER.increment(1);
            return;
        }

        try {
            MAPREDUCE_KEY.clear();
            OUTPUT_MAP_DOC.clear();
            final JSONObject inputJson  = new JSONObject(valueStr);

            // parse input JSON
            final JSONObject metadata = inputJson.getJSONObject(INPUT_METADATA_KEY);
            if (null == metadata) {
                throw new JSONException("Missing 'metadata'");
            }

            final JSONObject payload = inputJson.getJSONObject(INPUT_PAYLOAD_KEY);
            if (null == payload) {
                throw new JSONException("Missing 'payload'");
            }

            final JSONObject contentHeaders = payload.getJSONObject(INPUT_PAYLOAD_HEADERS_KEY);
            final String contentEncoding    = payload.getString(INPUT_PAYLOAD_ENCODING_KEY);
            final String contentBody        = payload.getString(INPUT_PAYLOAD_BODY_KEY);
            if (null == contentHeaders || null == contentEncoding || null == contentBody) {
                throw new JSONException("Missing one of 'payload/[headers|encoding|body]'");
            }

            if (!contentEncoding.equals("plain")) {
                BINARY_COUNTER.increment(1);
                LOG.info("Skipped binary record " + key);
                return;
            }

            // process WARC headers
            Iterator it = metadata.keys();
            while (it.hasNext()) {
                final String k = (String) it.next();
                if (k.equalsIgnoreCase("WARC-Record-ID")) {
                    final String recordId = metadata.getString(k);
                    WARC_RECORD_ID_VALUE.set(recordId);
                    OUTPUT_MAP_DOC.put(WARC_RECORD_ID_KEY, WARC_RECORD_ID_VALUE);
                    if (0 == MAPREDUCE_KEY.getLength()) {
                        MAPREDUCE_KEY.set(recordId);
                    }
                } else if (k.equalsIgnoreCase("WARC-TREC-ID")) {
                    final String trecId = metadata.getString(k);
                    WARC_TREC_ID_VALUE.set(trecId);
                    OUTPUT_MAP_DOC.put(WARC_TREC_ID_KEY, WARC_TREC_ID_VALUE);
                    if (trecId.startsWith("clueweb")) {
                        MAPREDUCE_KEY.set(trecId);
                    }
                } else if (k.equalsIgnoreCase("WARC-Target-URI")) {
                    try {
                        final URI targetURI = new URI(metadata.getString(k));
                        WARC_TARGET_HOSTNAME_VALUE.set(null != targetURI.getHost() ? targetURI.getHost() : "");
                        WARC_TARGET_PATH_VALUE.set(null != targetURI.getPath() ? targetURI.getPath() : "");
                        WARC_TARGET_QUERY_STRING_VALUE.set(null != targetURI.getQuery() ? targetURI.getQuery() : "");

                        OUTPUT_MAP_DOC.put(WARC_TARGET_HOSTNAME_KEY, WARC_TARGET_HOSTNAME_VALUE);
                        OUTPUT_MAP_DOC.put(WARC_TARGET_HOSTNAME_RAW_KEY, WARC_TARGET_HOSTNAME_RAW_VALUE);
                        OUTPUT_MAP_DOC.put(WARC_TARGET_PATH_KEY, WARC_TARGET_PATH_VALUE);
                        OUTPUT_MAP_DOC.put(WARC_TARGET_QUERY_STRING_KEY, WARC_TARGET_QUERY_STRING_VALUE);
                    } catch (URISyntaxException ignored) {}

                    WARC_TARGET_URI_VALUE.set(metadata.getString(k));
                    OUTPUT_MAP_DOC.put(WARC_TARGET_URI_KEY, WARC_TARGET_URI_VALUE);
                }
            }

            // process content (HTTP) headers
            it = contentHeaders.keys();
            while (it.hasNext()) {
                final String k = (String) it.next();
                if (k.equalsIgnoreCase("Content-Type")) {
                    final String[] splits = contentHeaders.getString(k).split(";");
                    CONTENT_TYPE_VALUE.set(splits[0].trim());
                    OUTPUT_MAP_DOC.put(CONTENT_TYPE_KEY, CONTENT_TYPE_VALUE);
                } else if (k.equalsIgnoreCase("Date")) {
                    final Calendar c = Calendar.getInstance();
                    final SimpleDateFormat dfInput  = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
                    final SimpleDateFormat dfOutput = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
                    try {
                        c.setTime(dfInput.parse(contentHeaders.getString(k)));
                        DATE_VALUE.set(dfOutput.format(c.getTime()));
                        OUTPUT_MAP_DOC.put(DATE_KEY, DATE_VALUE);
                    } catch (ParseException ignored) { }
                }
            }

            // create plaintext rendering from content body
            final Document jsoupDoc = Jsoup.parse(contentBody);
            final String renderedBody = HTML_TO_PLAIN_TEXT.getPlainText(jsoupDoc);
            // ignore document if rendered body is too small
            if (renderedBody.getBytes().length < 50) {
                LOG.warn("Document " + key + " with size " + renderedBody.getBytes().length + "bytes skipped (too small)");
                TOO_SMALL_COUNTER.increment(1);
                return;
            }

            // language detection
            String lang;
            try {
                lang = LANGUAGE_DETECTOR.detect(renderedBody);
            } catch (IOException e) {
                lang = "en";
                LOG.warn("Language detection for document " + key + "failed, falling back to en");
                LANGDETECT_FAILED_COUNTER.increment(1);
            }
            LANG_VALUE.set(lang);
            OUTPUT_MAP_DOC.put(LANG_KEY, LANG_VALUE);

            // add rendered body to output document
            BODY_LENGTH_VALUE.set(renderedBody.length());
            BODY_VALUE.set(renderedBody);
            OUTPUT_MAP_DOC.put(BODY_LENGTH_KEY, BODY_LENGTH_VALUE);
            OUTPUT_MAP_DOC.put(new Text(BODY_BASE_KEY + LANG_VALUE), BODY_VALUE);

            // parse title and meta tags within body source
            Source bodySource = new Source(contentBody);
            TITLE_VALUE.set(getDocTitle(bodySource, 90));
            META_DESC_VALUE.set(getMetaTagContents(bodySource, "name", "description", 400));
            META_KEYWORDS_VALUE.set(getMetaTagContents(bodySource, "name", "keywords", 400));

            OUTPUT_MAP_DOC.put(new Text(TITLE_BASE_KEY + LANG_VALUE),     TITLE_VALUE);
            OUTPUT_MAP_DOC.put(new Text(META_BASE_DESC_KEY + LANG_VALUE), META_DESC_VALUE);
            OUTPUT_MAP_DOC.put(META_KEYWORDS_KEY,                         META_KEYWORDS_VALUE);

            // write final document to context
            context.write(MAPREDUCE_KEY, OUTPUT_MAP_DOC);
        } catch (StackOverflowError e) {
            // HTML too deeply nested
            LOG.warn("Document " + key + " with deep HTML tag nesting level skipped");
            TOO_DEEP_COUNTER.increment(1);
        } catch (JSONException e) {
            LOG.warn("Document " + key + " skipped due to JSON parsing error: " + e.getMessage());
            PARSE_ERROR_COUNTER.increment(1);
        }
    }

    /**
     * Get title from source document.
     *
     * @param source Jericho Source object
     * @param maxLength maximum length of content to return, content that is longer will be truncated
     * @return document title
     */
    private String getDocTitle(final Source source, final int maxLength)
    {
        String title = "";
        try {
            final List<net.htmlparser.jericho.Element> titleElements = source.getAllElements("title");
            if (0 != titleElements.size()) {
                title = titleElements.
                        get(0).
                        getTextExtractor().
                        setIncludeAttributes(false).
                        toString().
                        trim();
            }

            if (title.isEmpty()) {
                // use body as title if no real title found
                title = source.
                        getTextExtractor().
                        setIncludeAttributes(false).
                        toString().
                        trim();
            }
        } catch (NullPointerException ignored) { }

        // truncate title to maxLength characters
        return truncateSnippet(title, maxLength);
    }

    /**
     * Get meta tag contents from source document.
     *
     * @param source Jericho Source object
     * @param type which type of meta data to get (usually "name" or "http-equiv")
     * @param what what content of type "type" to get (e.g. "description" or "keywords")
     * @param maxLength maximum length of content to return, content that is longer will be truncated (-1 for no limit)
     * @return meta tag contents, empty string of none found
     */
    private String getMetaTagContents(final Source source, final String type, final String what, final int maxLength)
    {
        String metaTagContents = "";

        try {
            final List<net.htmlparser.jericho.Element> metaElements = source.getAllElements("meta");
            if (0 != metaElements.size()) {
                for (final net.htmlparser.jericho.Element e : metaElements) {
                    final String typeAttr = e.getAttributeValue(type);
                    final String contentAttr = e.getAttributeValue("content");
                    if (null != typeAttr && null != contentAttr &&
                            typeAttr.trim().toLowerCase().equals(what.trim().toLowerCase())) {
                        metaTagContents = contentAttr;
                        break;
                    }
                }
            }

            if (-1 != maxLength) {
                return truncateSnippet(metaTagContents, maxLength);
            }
        } catch (NullPointerException ignored) { }

        return metaTagContents.trim();
    }

    /**
     * Truncate a snippet after a certain number of characters, trying to preserve full words.
     * Will cut the string hard after the specified amount of characters if no spaces could be
     * found or cutting after words would reduce the size more than 2/3 of the desired length.
     *
     * @param snippet the snippet
     * @param numCharacters number of characters after which to truncate
     * @return the truncated snippet
     */
    private String truncateSnippet(String snippet, final int numCharacters)
    {
        if (snippet.length() > numCharacters) {
            final boolean wordEnded = (snippet.charAt(numCharacters) == ' ');
            snippet = snippet.substring(0, numCharacters);

            // get rid of incomplete words
            final int pos = snippet.lastIndexOf(' ');
            if (!wordEnded && -1 != pos) {
                // shorten snippet if it doesn't become too short then
                if ((int)(.6 * numCharacters) <= pos) {
                    snippet = snippet.substring(0, pos);
                }
            }
        }

        return snippet.trim();
    }
}