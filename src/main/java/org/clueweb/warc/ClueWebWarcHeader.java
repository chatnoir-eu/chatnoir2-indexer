package org.clueweb.warc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Public interface for WarcRecord headers
 */
public interface ClueWebWarcHeader
{
    /**
     * Serialization output
     *
     * @param out the data output stream
     * @throws java.io.IOException
     */
    public void write(DataOutput out) throws IOException;

    /**
     * Serialization input
     *
     * @param in the data input stream
     * @throws java.io.IOException
     */
    public void readFields(DataInput in) throws IOException;

    public String toString();
}
