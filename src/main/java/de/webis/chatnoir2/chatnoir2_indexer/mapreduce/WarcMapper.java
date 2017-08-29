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

import de.webis.WebisUUID;
import de.webis.chatnoir2.chatnoir2_indexer.util.ContentExtractor;
import de.webis.chatnoir2.chatnoir2_indexer.util.LangDetector;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.helper.StringUtil;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

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
    protected static Counter JSON_PARSE_ERROR_COUNTER;
    protected static Counter TOO_LARGE_COUNTER;
    protected static Counter TOO_SMALL_COUNTER;
    protected static Counter HTML_PARSER_ERROR_COUNTER;
    protected static Counter BINARY_COUNTER;
    protected static Counter LANGDETECT_FAILED_COUNTER;
    protected static Counter SKIPPED_NO_ID_COUNTER;

    protected static LangDetector LANGUAGE_DETECTOR = null;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
        super.setup(context);

        RECORDS_COUNTER             = context.getCounter(RecordCounters.RECORDS);
        JSON_PARSE_ERROR_COUNTER    = context.getCounter(RecordCounters.SKIPPED_RECORDS_JSON_PARSE_ERROR);
        TOO_LARGE_COUNTER           = context.getCounter(RecordCounters.SKIPPED_RECORDS_TOO_LARGE);
        TOO_SMALL_COUNTER           = context.getCounter(RecordCounters.SKIPPED_RECORDS_TOO_SMALL);
        HTML_PARSER_ERROR_COUNTER   = context.getCounter(RecordCounters.SKIPPED_RECORDS_HTML_PARSE_ERROR);
        BINARY_COUNTER              = context.getCounter(RecordCounters.SKIPPED_RECORDS_BINARY);
        LANGDETECT_FAILED_COUNTER   = context.getCounter(RecordCounters.LANGDETECT_FAILED);
        SKIPPED_NO_ID_COUNTER       = context.getCounter(RecordCounters.SKIPPED_RECORDS_NO_ID);

        if (null == LANGUAGE_DETECTOR) {
            LANGUAGE_DETECTOR = new LangDetector();
        }
    }

    @Override
    public void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException
    {
        OUTPUT_MAP.clear();
        MAPREDUCE_KEY.clear();

        RECORDS_COUNTER.increment(1);
        final String valueStr = value.toString();

        LOG.debug("Mapping document " + key);

        // ignore large files
        if (valueStr.getBytes().length > 1024 * 1024) {
            LOG.warn("Skipped document " + key + " with size " + valueStr.getBytes().length + "bytes (too large)");
            TOO_LARGE_COUNTER.increment(1);
            return;
        }

        try {
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
            String recordId = null;
            String trecId = null;
            while (it.hasNext()) {
                final String k = (String) it.next();
                if (k.equalsIgnoreCase("WARC-Record-ID")) {
                    recordId = metadata.getString(k);
                    WARC_RECORD_ID_VALUE.set(recordId);
                    OUTPUT_MAP.put(WARC_RECORD_ID_KEY, WARC_RECORD_ID_VALUE);
                } else if (k.equalsIgnoreCase("WARC-TREC-ID")) {
                    trecId = metadata.getString(k);
                    WARC_TREC_ID_VALUE.set(trecId);
                    OUTPUT_MAP.put(WARC_TREC_ID_KEY, WARC_TREC_ID_VALUE);
                } else if (k.equalsIgnoreCase("WARC-Target-URI")) {
                    try {
                        final URI targetURI = new URI(metadata.getString(k));

                        WARC_TARGET_HOSTNAME_VALUE.set(null != targetURI.getHost() ? targetURI.getHost() : "");
                        OUTPUT_MAP.put(WARC_TARGET_HOSTNAME_KEY, WARC_TARGET_HOSTNAME_VALUE);

                        WARC_TARGET_PATH_VALUE.set(null != targetURI.getPath() ? targetURI.getPath() : "");
                        OUTPUT_MAP.put(WARC_TARGET_PATH_KEY, WARC_TARGET_PATH_VALUE);

                        WARC_TARGET_QUERY_STRING_VALUE.set(null != targetURI.getQuery() ? targetURI.getQuery() : "");
                        OUTPUT_MAP.put(WARC_TARGET_QUERY_STRING_KEY, WARC_TARGET_QUERY_STRING_VALUE);
                    } catch (URISyntaxException ignored) {
                        LOG.error("URL Exception for url '" + metadata.getString(k) + "': " + ignored.getMessage());
                    }

                    WARC_TARGET_URI_VALUE.set(metadata.getString(k));
                    OUTPUT_MAP.put(WARC_TARGET_URI_KEY, WARC_TARGET_URI_VALUE);
                }
            }

            if (null == recordId && null != trecId) {
                recordId = trecId;
            } else if (null == recordId) {
                SKIPPED_NO_ID_COUNTER.increment(1);
                LOG.warn("Document skipped, because it has no ID");
                return;
            }

            if (null != trecId) {
                MAPREDUCE_KEY.set(trecId);
            } else {
                MAPREDUCE_KEY.set(recordId);
            }

            DOCUMENT_UUID_VALUE.set(WebisUUID.generateUUID(
                    context.getConfiguration().get("webis.mapfile.uuid.prefix"), MAPREDUCE_KEY.toString()).toString());
            OUTPUT_MAP.put(DOCUMENT_UUID_KEY, DOCUMENT_UUID_VALUE);

            // process content (HTTP) headers
            it = contentHeaders.keys();
            while (it.hasNext()) {
                final String k = (String) it.next();
                if (k.equalsIgnoreCase("Content-Type")) {
                    final String[] splits = contentHeaders.getString(k).split(";");
                    CONTENT_TYPE_VALUE.set(splits[0].trim());
                    OUTPUT_MAP.put(CONTENT_TYPE_KEY, CONTENT_TYPE_VALUE);
                } else if (k.equalsIgnoreCase("Date")) {
                    final Calendar c = Calendar.getInstance();
                    final SimpleDateFormat dfInput  = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
                    final SimpleDateFormat dfOutput = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
                    try {
                        c.setTime(dfInput.parse(contentHeaders.getString(k)));
                        DATE_VALUE.set(dfOutput.format(c.getTime()));
                        OUTPUT_MAP.put(DATE_KEY, DATE_VALUE);
                    } catch (ParseException ignored) { }
                }
            }

            // full content extraction (all text nodes)
            String fullContent = ContentExtractor.extractEverything(contentBody);

            // language detection
            String lang;
            lang = LANGUAGE_DETECTOR.detect(fullContent);
            if (lang.isEmpty()) {
                lang = "unknown";
                LOG.warn("Language detection for document " + key + " failed");
                LANGDETECT_FAILED_COUNTER.increment(1);
            }

            LANG_VALUE.set(lang);
            OUTPUT_MAP.put(LANG_KEY, LANG_VALUE);

            // create plaintext rendering from content body
            String mainContent;
            if (lang.equalsIgnoreCase("en")) {
                mainContent = ContentExtractor.extract(contentBody, "en");
            } else {
                mainContent = ContentExtractor.extract(contentBody, lang, "en");
            }
            if (null == mainContent || mainContent.getBytes().length < 5) {
                int size = null != mainContent ? mainContent.getBytes().length : 0;
                LOG.warn("Document " + key + " with size " + size + " bytes skipped (too small)");
                TOO_SMALL_COUNTER.increment(1);
                return;
            }
            String headings = ContentExtractor.extractHeadings(contentBody, 3);

            // add extracted body to output document
            BODY_LENGTH_VALUE.set(mainContent.length());
            OUTPUT_MAP.put(BODY_LENGTH_KEY, BODY_LENGTH_VALUE);

            BODY_VALUE.set(mainContent);
            OUTPUT_MAP.put(new Text(BODY_KEY_PREFIX + lang), BODY_VALUE);

            FULL_BODY_VALUE.set(fullContent);
            OUTPUT_MAP.put(new Text(FULL_BODY_KEY_PREFIX + lang), FULL_BODY_VALUE);

            HEADINGS_VALUE.set(headings);
            OUTPUT_MAP.put(new Text(HEADINGS_KEY_PREFIX + lang), HEADINGS_VALUE);

            // parse title and meta tags within body source
            try {
                Document bodyDoc = Jsoup.parse(contentBody);
                TITLE_VALUE.set(getDocTitle(bodyDoc, 90));
                OUTPUT_MAP.put(new Text(TITLE_KEY_PREFIX + lang), TITLE_VALUE);

                META_DESC_VALUE.set(getMetaTagContents(bodyDoc, "name", "description", 400));
                OUTPUT_MAP.put(new Text(META_DESC_KEY_PREFIX + lang), META_DESC_VALUE);

                META_KEYWORDS_VALUE.set(getMetaTagContents(bodyDoc, "name", "keywords", 400));
                OUTPUT_MAP.put(META_KEYWORDS_KEY, META_KEYWORDS_VALUE);
            } catch (Exception e) {
                LOG.warn("HTML parsing of document" + key + " failed");
                HTML_PARSER_ERROR_COUNTER.increment(1);
            }

            // write final document to context
            context.write(MAPREDUCE_KEY, OUTPUT_MAP);
        } catch (JSONException e) {
            LOG.error("Document " + key + " skipped due to JSON parsing error: " + e.getMessage());
            JSON_PARSE_ERROR_COUNTER.increment(1);
        }
    }

    /**
     * Get title from source document or text contents of the HTML body if no title exists.
     *
     * @param doc Jsoup Document
     * @param maxLength maximum length of content to return, content that is longer will be truncated
     * @return document title
     */
    private String getDocTitle(final Document doc, final int maxLength)
    {
        String title = doc.title();
        if (title.isEmpty()) {
            Elements elements = doc.getElementsByTag("body");
            if (elements.size() > 0) {
                title =  StringUtil.normaliseWhitespace(elements.get(0).text().trim());
            }
        }

        return truncateSnippet(title, maxLength);
    }

    /**
     * Get meta tag contents from source document.
     *
     * @param doc Jsoup Document
     * @param type which type of meta data to get (usually "name" or "http-equiv")
     * @param what what content of type "type" to get (e.g. "description" or "keywords")
     * @param maxLength maximum length of content to return, content that is longer will be truncated (-1 for no limit)
     * @return meta tag contents, empty string of none found
     */
    private String getMetaTagContents(final Document doc, final String type, final String what, final int maxLength)
    {
        String metaTagContents = "";

        Elements metaTags = doc.getElementsByTag("meta");
        for (Element e: metaTags) {
            if (e.hasAttr(type) && e.hasAttr("content") && e.attr(type).equals(what)) {
                metaTagContents = StringUtil.normaliseWhitespace(e.attr("content").trim());
                break;
            }
        }

        if (-1 != maxLength) {
            return truncateSnippet(metaTagContents, maxLength);
        }

        return metaTagContents;
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
                if ((int) (.6 * numCharacters) <= pos) {
                    snippet = snippet.substring(0, pos);
                }
            }
        }

        return snippet.trim();
    }
}