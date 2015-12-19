package org.clueweb.mapreduce;

import org.apache.hadoop.io.*;

/**
 * Base interface for Warc JSON mappers and reducers.
 *
 * @author Janek Bevendorff
 */
public interface WarcMapReduceBase
{
    String INPUT_METADATA_KEY         = "metadata";
    String INPUT_PAYLOAD_KEY          = "payload";
    String INPUT_PAYLOAD_BODY_KEY     = "body";
    String INPUT_PAYLOAD_HEADERS_KEY  = "headers";
    String INPUT_PAYLOAD_ENCODING_KEY = "encoding";

    Text WARC_TREC_ID_KEY             = new Text("warc_trec_id");
    Text WARC_RECORD_ID_KEY           = new Text("warc_record_id");
    Text WARC_TARGET_URI_KEY          = new Text("warc_target_uri");
    Text WARC_TARGET_HOSTNAME_KEY     = new Text("warc_target_hostname");
    Text WARC_TARGET_HOSTNAME_RAW_KEY = new Text("warc_target_hostname_raw");
    Text WARC_TARGET_PATH_KEY         = new Text("warc_target_path");
    Text WARC_TARGET_QUERY_STRING_KEY = new Text("warc_target_query_string");
    Text CONTENT_TYPE_KEY             = new Text("content_type");
    Text META_KEYWORDS_KEY            = new Text("meta_keywords");
    Text LANG_KEY                     = new Text("lang");
    Text DATE_KEY                     = new Text("date");
    Text SPAM_RANK_KEY                = new Text("spam_rank");
    Text PAGE_RANK_KEY                = new Text("page_rank");
    Text ANCHOR_TEXTS_KEY             = new Text("anchor_texts");
    Text BODY_LENGTH_KEY              = new Text("body_length");

    String TITLE_BASE_KEY     = "title_lang_";
    String META_BASE_DESC_KEY = "meta_desc_lang_";
    String BODY_BASE_KEY      = "body_lang_";

    Text WARC_TREC_ID_VALUE             = new Text();
    Text WARC_RECORD_ID_VALUE           = new Text();
    Text WARC_TARGET_URI_VALUE          = new Text();
    Text WARC_TARGET_HOSTNAME_VALUE     = new Text();
    Text WARC_TARGET_HOSTNAME_RAW_VALUE = new Text();
    Text WARC_TARGET_PATH_VALUE         = new Text();
    Text WARC_TARGET_QUERY_STRING_VALUE = new Text();
    Text CONTENT_TYPE_VALUE             = new Text();
    Text META_KEYWORDS_VALUE            = new Text();
    Text LANG_VALUE                     = new Text();
    Text DATE_VALUE                     = new Text();
    Text TITLE_VALUE                    = new Text();
    Text META_DESC_VALUE                = new Text();
    Text BODY_VALUE                     = new Text();
    LongWritable BODY_LENGTH_VALUE      = new LongWritable();
    IntWritable SPAM_RANK_VALUE         = new IntWritable();
    DoubleWritable PAGE_RANK_VALUE      = new DoubleWritable();
    ArrayWritable ANCHOR_TEXTS_VALUE    = new ArrayWritable(new String[]{});

    MapWritable OUTPUT_MAP_DOC    = new MapWritable();
    BytesWritable OUTPUT_JSON_DOC = new BytesWritable();

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
        SKIPPED_RECORDS_PARSE_ERROR,

        /**
         * Number of skipped records that are too large.
         */
        SKIPPED_RECORDS_TOO_LARGE,

        /**
         * Number of skipped records that are too small.
         */
        SKIPPED_RECORDS_TOO_SMALL,

        /**
         * Number of skipped records that are too deeply nested.
         */
        SKIPPED_RECORDS_TOO_DEEP,

        /**
         * Number of skipped binary records.
         */
        SKIPPED_RECORDS_BINARY,

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