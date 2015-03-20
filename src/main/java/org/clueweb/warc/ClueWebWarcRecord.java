package org.clueweb.warc;

import java.util.Map;
import java.util.Set;

/**
 * Public interface for ClueWebWarcRecords.
 */
public interface ClueWebWarcRecord
{
    /**
     * Retrieve the total record length (header and content).
     *
     * @return total record length
     */
    public int getTotalRecordLength();

    /**
     * Get the file path from this WARC file (if set).
     */
    public String getWarcFilePath();

    /**
     * Get the set of metadata items from the header.
     */
    public Set<Map.Entry<String, String>> getHeaderMetadata();

    /**
     * Get a value for a specific header metadata key.
     *
     * @param key key of the entry
     */
    public String getHeaderMetadataItem(String key);

    /**
     * Retrieve the byte content for this record.
     */
    public byte[] getByteContent();

    /**
     * Retrieve the bytes content as a UTF-8 string
     */
    public String getContentUTF8();

    /**
     * Get the header record type string.
     */
    public String getHeaderRecordType();

    /**
     * Get the WARC header as a string
     */
    public String getHeaderString();

    /**
     * Get Warc document ID.
     */
    public String getDocid();

    /**
     * Get Warc document contents.
     */
    public String getContent();
}
