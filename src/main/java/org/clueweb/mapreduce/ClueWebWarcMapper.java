package org.clueweb.mapreduce;

import net.htmlparser.jericho.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.clueweb.warc.ClueWebWarcRecord;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * MapReduce Mapper class for ClueWeb WARC records.
 *
 * @author Janek Bevendorff
 * @version 1
 */
public class ClueWebWarcMapper extends Mapper<LongWritable, ClueWebWarcRecord, Text, MapWritable>
{
    /**
     * Counter enum.
     */
    public static enum Records { PAGES }

    public void map(final LongWritable key, final ClueWebWarcRecord value, final Context context) throws IOException, InterruptedException
    {
        // disable Jericho log
        Config.LoggerProvider = LoggerProvider.DISABLED;

        final String docId = value.getDocid();

        if (null != docId) {
            final Text docIdText    = new Text(docId);
            final MapWritable doc   = new MapWritable();

            // headers
            Set<Map.Entry<String, String>> headers = value.getHeaderMetadata();
            for (Map.Entry<String, String> entry : headers) {
                switch (entry.getKey()) {
                    case "WARC-TREC-ID":
                    case "WARC-Target-URI":
                    case "WARC-Warcinfo-ID":
                        doc.put(new Text(entry.getKey()), new Text(entry.getValue()));
                }

            }

            // contents
            final String rawHTML      = value.getContent();
            final Source bodySource   = new Source(rawHTML);
            final String renderedBody = renderHTMLToText(bodySource);

            doc.put(new Text("WARC-TREC-ID"), docIdText);
            doc.put(new Text("title"),         new Text(getDocTitle(bodySource, 90)));
            doc.put(new Text("meta_desc"),     new Text(getMetaTagContents(bodySource, "name", "description", 400)));
            doc.put(new Text("meta_keywords"), new Text(getMetaTagContents(bodySource, "name", "keywords", 400)));
            //doc.put(new Text("raw_html"),      new Text(rawHTML.trim()));
            doc.put(new Text("body"),          new Text(renderedBody));
            doc.put(new Text("body_length"),   new LongWritable(renderedBody.length()));

            context.getCounter(Records.PAGES).increment(1);
            context.write(docIdText, doc);
        }
    }

    /**
     * Render plaintext from Source document.
     *
     * @param source Jericho Source object
     * @return parsed plain text
     */
    private String renderHTMLToText(final Source source)
    {
        return source.
                getRenderer().
                setIncludeHyperlinkURLs(false).
                setHRLineLength(0).
                setNewLine("\n").
                setBlockIndentSize(0).
                toString().
                trim();
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
        String title;
        final List<Element> titleElements = source.getAllElements("title");
        if (0 != titleElements.size()) {
            title = titleElements.
                    get(0).
                    getTextExtractor().
                    setIncludeAttributes(false).
                    toString().
                    trim();
        } else {
            // use body as title if no real title found
            title = source.
                    getTextExtractor().
                    setIncludeAttributes(false).
                    toString().
                    trim();
        }

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

        final List<Element> metaElements = source.getAllElements("meta");
        if (0 != metaElements.size()) {
            for (final Element e : metaElements) {
                final String typeAttr    = e.getAttributeValue(type);
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