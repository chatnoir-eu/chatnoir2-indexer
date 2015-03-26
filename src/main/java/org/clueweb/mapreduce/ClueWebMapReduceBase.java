package org.clueweb.mapreduce;

import org.apache.hadoop.io.*;

/**
 * Base class of ClueWeb mappers.
 *
 * @author Janek Bevendorff
 * @version 1
 */
public interface ClueWebMapReduceBase
{
    public static final Text warcTrecIdKey    = new Text("WARC-TREC-ID");
    public static final Text warcInfoIdKey    = new Text("WARC-Warcinfo-ID");
    public static final Text warcTargetUriKey = new Text("WARC-Target-URI");
    public static final Text titleKey         = new Text("title");
    public static final Text metaDescKey      = new Text("meta_desc");
    public static final Text metaKeywordsKey  = new Text("meta_keywords");
    public static final Text bodyKey          = new Text("body");
    public static final Text bodyLengthKey    = new Text("body_length");
    public static final Text spamRankKey      = new Text("spam_rank");
    public static final Text pageRankKey      = new Text("page_rank");
    public static final Text anchorTextKey    = new Text("anchor_texts");

    public static final MapWritable outputDoc = new MapWritable();

    public static final Text emptyText                   = new Text();
    public static final LongWritable emptyLongWritable   = new LongWritable();
    public static final FloatWritable emptyFloatWritable = new FloatWritable();
    public static final ArrayWritable emptyArrayWritable = new ArrayWritable(new String[0]);

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
