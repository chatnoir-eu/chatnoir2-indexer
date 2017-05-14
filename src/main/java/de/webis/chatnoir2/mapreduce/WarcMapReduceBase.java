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

    Text MAPREDUCE_KEY = new Text();

    MapWritable OUTPUT_MAP_DOC    = new MapWritable();
    Text OUTPUT_JSON_DOC          = new Text();

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
