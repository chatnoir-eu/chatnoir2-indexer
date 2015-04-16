package org.clueweb.mapreduce;

import org.apache.hadoop.io.*;

/**
 * Base interface for ClueWeb mappers and reducers.
 *
 * @author Janek Bevendorff
 * @version 1
 */
public interface ClueWebMapReduceBase
{
    Text WARC_TREC_ID_KEY    = new Text("WARC-TREC-ID");
    Text WARC_INFO_ID_KEY    = new Text("WARC-Warcinfo-ID");
    Text WARC_TARGET_URI_KEY = new Text("WARC-Target-URI");
    Text LANG_KEY            = new Text("lang");
    Text TITLE_KEY           = new Text("title");
    Text META_DESC_KEY       = new Text("meta_desc");
    Text META_KEYWORDS_KEY   = new Text("meta_keywords");
    Text BODY_KEY            = new Text("body");
    Text BODY_LENGTH_KEY     = new Text("body_length");
    Text SPAM_RANK_KEY       = new Text("spam_rank");
    Text PAGE_RANK_KEY       = new Text("page_rank");
    Text ANCHOR_TEXTS_KEY    = new Text("anchor_texts");

    MapWritable OUTPUT_DOC = new MapWritable();

    Text EMPTY_TEXT                    = new Text();
    LongWritable EMPTY_LONG_WRITABLE   = new LongWritable();
    FloatWritable EMPTY_FLOAT_WRITABLE = new FloatWritable();
    ArrayWritable EMPTY_ARRAY_WRITABLE = new ArrayWritable(new String[0]);

    /**
     * MapReduce counters.
     */
    public static enum RecordCounters {
        /**
         * Total records read.
         */
        RECORDS,

        /**
         * Number of skipped records due to null ID.
         */
        SKIPPED_RECORDS_NULL_ID,

        /**
         * Number of skipped records that are too large.
         */
        SKIPPED_RECORDS_TOO_LARGE,

        /**
         * Number of skipped records that are too deeply nested.
         */
        SKIPPED_RECORDS_TOO_DEEP,

        /**
         * Number of documents with no HTML content.
         */
        NO_HTML,

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
