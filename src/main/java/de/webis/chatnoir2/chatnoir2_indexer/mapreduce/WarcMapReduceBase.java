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

package de.webis.chatnoir2.chatnoir2_indexer.mapreduce;

import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

/**
 * Base interface for Warc JSON mappers and reducers.
 *
 * @author Janek Bevendorff
 */
public interface WarcMapReduceBase
{
    Logger LOG = Logger.getLogger(WarcMapper.class);

    String INPUT_METADATA_KEY         = "metadata";
    String INPUT_PAYLOAD_KEY          = "payload";
    String INPUT_PAYLOAD_BODY_KEY     = "body";
    String INPUT_PAYLOAD_HEADERS_KEY  = "headers";
    String INPUT_PAYLOAD_ENCODING_KEY = "encoding";

    Text MAPREDUCE_KEY = new Text();

    Text DOCUMENT_UUID_KEY   = new Text("uuid");
    Text DOCUMENT_UUID_VALUE = new Text();

    Text WARC_TREC_ID_KEY             = new Text("warc_trec_id");
    Text WARC_RECORD_ID_KEY           = new Text("warc_record_id");
    Text WARC_TARGET_URI_KEY          = new Text("warc_target_uri");
    Text WARC_TARGET_HOSTNAME_KEY     = new Text("warc_target_hostname");
    Text WARC_TARGET_PATH_KEY         = new Text("warc_target_path");
    Text WARC_TARGET_QUERY_STRING_KEY = new Text("warc_target_query_string");

    Text CONTENT_TYPE_KEY  = new Text("content_type");
    Text META_KEYWORDS_KEY = new Text("meta_keywords");
    Text LANG_KEY          = new Text("lang");
    Text DATE_KEY          = new Text("date");
    Text SPAM_RANK_KEY     = new Text("spam_rank");
    Text PAGE_RANK_KEY     = new Text("page_rank");
    Text BODY_LENGTH_KEY   = new Text("body_length");

    String TITLE_KEY_PREFIX        = "title_lang.";
    String META_DESC_KEY_PREFIX    = "meta_desc_lang.";
    String BODY_KEY_PREFIX         = "body_lang.";
    String FULL_BODY_KEY_PREFIX    = "full_body_lang.";
    String HEADINGS_KEY_PREFIX     = "headings_lang.";
    String ANCHOR_TEXTS_KEY_PREFIX = "anchor_texts_lang.";

    Text WARC_TREC_ID_VALUE             = new Text();
    Text WARC_RECORD_ID_VALUE           = new Text();
    Text WARC_TARGET_URI_VALUE          = new Text();
    Text WARC_TARGET_HOSTNAME_VALUE     = new Text();
    Text WARC_TARGET_PATH_VALUE         = new Text();
    Text WARC_TARGET_QUERY_STRING_VALUE = new Text();

    Text CONTENT_TYPE_VALUE          = new Text();
    Text META_KEYWORDS_VALUE         = new Text();
    Text LANG_VALUE                  = new Text();
    Text DATE_VALUE                  = new Text();
    LongWritable SPAM_RANK_VALUE     = new LongWritable();
    FloatWritable PAGE_RANK_VALUE    = new FloatWritable();
    LongWritable BODY_LENGTH_VALUE   = new LongWritable();
  
    Text TITLE_VALUE        = new Text();
    Text META_DESC_VALUE    = new Text();
    Text BODY_VALUE         = new Text();
    Text FULL_BODY_VALUE    = new Text();
    Text HEADINGS_VALUE     = new Text();
    Text ANCHOR_TEXTS_VALUE = new Text();

    MapWritable OUTPUT_MAP = new MapWritable();

    /**
     * MapReduce counters.
     */
    enum RecordCounters {
        /**
         * Total records read.
         */
        RECORDS,

        /**
         * Number of skipped records due to JSON parse errors.
         */
        SKIPPED_RECORDS_JSON_PARSE_ERROR,

        /**
         * Number of skipped records that are too large.
         */
        SKIPPED_RECORDS_TOO_LARGE,

        /**
         * Number of skipped records that are too small.
         */
        SKIPPED_RECORDS_TOO_SMALL,

        /**
         * Number of skipped records due to HTML parser errors.
         */
        SKIPPED_RECORDS_HTML_PARSE_ERROR,

        /**
         * Number of skipped binary records.
         */
        SKIPPED_RECORDS_BINARY,

        /**
         * Record skipped because it has no valid ID.
         */
        SKIPPED_RECORDS_NO_ID,

        /**
         * Number documents for which language detection failed.
         */
        LANGDETECT_FAILED,

        /**
         * Number of actual JSON docs generated.
         */
        GENERATED_DOCS,

        /**
         * Number of documents with no plain-text content after reduce stage.
         */
        NO_CONTENT
    }
}
