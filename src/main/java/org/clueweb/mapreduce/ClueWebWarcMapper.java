package org.clueweb.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.clueweb.app.HtmlToPlainText;
import org.clueweb.warc.ClueWebWarcRecord;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * MapReduce Mapper class for ClueWeb WARC records.
 *
 * @author Janek Bevendorff
 * @version 1
 */
public class ClueWebWarcMapper extends Mapper<LongWritable, ClueWebWarcRecord, Text, MapWritable> implements ClueWebMapReduceBase
{
    protected static final Logger LOG = Logger.getLogger(ClueWebWarcMapper.class);

    protected static Counter recordsCounter;
    protected static Counter tooLargeCounter;
    protected static Counter tooDeepCounter;
    protected static Counter nullIdCounter;

    protected static final Text WARC_TREC_ID_VALUE = new Text();
    protected static final Text WARC_INFO_ID_VALUE = new Text();
    protected static final Text WARC_TARGET_URI_VALUE = new Text();
    protected static final Text LANG_VALUE = new Text();
    protected static final Text TITLE_VALUE = new Text();
    protected static final Text META_DESC_VALUE = new Text();
    protected static final Text META_KEYWORDS_VALUE = new Text();
    protected static final Text BODY_VALUE = new Text();
    protected static final LongWritable BODY_LENGTH_VALUE = new LongWritable();

    protected static  final HtmlToPlainText HTML_TO_PLAIN_TEXT = new HtmlToPlainText();

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
        super.setup(context);

        recordsCounter  = context.getCounter(RecordCounters.RECORDS);
        tooLargeCounter = context.getCounter(RecordCounters.SKIPPED_RECORDS_TOO_LARGE);
        tooDeepCounter  = context.getCounter(RecordCounters.SKIPPED_RECORDS_TOO_DEEP);
        nullIdCounter   = context.getCounter(RecordCounters.SKIPPED_RECORDS_NULL_ID);

        // disable Jericho log
        net.htmlparser.jericho.Config.LoggerProvider = net.htmlparser.jericho.LoggerProvider.DISABLED;
    }

    public void map(final LongWritable key, final ClueWebWarcRecord value, final Context context) throws IOException, InterruptedException
    {
        recordsCounter.increment(1);
        OUTPUT_DOC.clear();

        final String docId = value.getDocid();

        if (null == docId) {
            LOG.warn(String.format("Skipped document #%d with null ID", key.get()));
            nullIdCounter.increment(1);
            return;
        }
        WARC_TREC_ID_VALUE.set(docId);

        LOG.debug(String.format("Mapping document %s", docId));

        // ignore large files
        if (value.getByteContent().length > 4 * 1024 * 1024) {
            LOG.warn(String.format("Document %s with size %dbytes skipped", docId, value.getByteContent().length));
            tooLargeCounter.increment(1);
            return;
        }

        // WARC headers
        final Set<Map.Entry<String, String>> headers = value.getHeaderMetadata();
        for (final Map.Entry<String, String> entry : headers) {
            final String k = entry.getKey();
            if (k.equals("WARC-Target-URI")) {
                WARC_TARGET_URI_VALUE.set(entry.getValue());
                OUTPUT_DOC.put(WARC_TARGET_URI_KEY, WARC_TARGET_URI_VALUE);
            } else if (k.equals("WARC-Warcinfo-ID")) {
                WARC_INFO_ID_VALUE.set(entry.getValue());
                OUTPUT_DOC.put(WARC_INFO_ID_KEY, WARC_INFO_ID_VALUE);
            }
        }

        net.htmlparser.jericho.Source bodySource = null;
        try {
            final String rawHTML = value.getContent();

            bodySource                = new net.htmlparser.jericho.Source(rawHTML);
            /*final Renderer renderer   = getHTMLToTextRenderer(bodySource);
            final long estimatedSized = renderer.getEstimatedMaximumOutputLength();
            if (estimatedSized > 20 * 1024 * 1024) {
                LOG.warn(String.format("Document %s with estimated rendered size of %dbytes skipped", docId, estimatedSized));
                tooLargeCounter.increment(1);
                return;
            }*/

            //final String renderedBody = renderer.toString().trim();
            final Document jsoupDoc = Jsoup.parse(rawHTML);
            final String renderedBody = HTML_TO_PLAIN_TEXT.getPlainText(jsoupDoc);

            TITLE_VALUE.set(getDocTitle(bodySource, 90));
            META_DESC_VALUE.set(getMetaTagContents(bodySource, "name", "description", 400));
            META_KEYWORDS_VALUE.set(getMetaTagContents(bodySource, "name", "keywords", 400));
            BODY_VALUE.set(renderedBody);
            BODY_LENGTH_VALUE.set(renderedBody.length());

            // language detection
            final URL url            = new URL("http://betaweb020.medien.uni-weimar.de:9200/_langdetect");
            final URLConnection conn = url.openConnection();
            conn.setDoOutput(true);
            final PrintStream ps = new PrintStream(conn.getOutputStream());
            ps.print(renderedBody);
            ps.close();

            BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            final StringBuilder strBuilder = new StringBuilder();
            while (null != (line = br.readLine())) {
                strBuilder.append(line);
            }
            br.close();

            String lang = "en";
            final JSONObject json;
            try {
                json = new JSONObject(strBuilder.toString());
                lang = json.getJSONArray("languages").getJSONObject(0).getString("language");
            } catch (JSONException e) {
                LOG.warn(String.format("JSON error: %s", e.getMessage()));
            }
            LANG_VALUE.set(lang);

            OUTPUT_DOC.put(LANG_KEY,          LANG_VALUE);
            OUTPUT_DOC.put(WARC_TREC_ID_KEY,  WARC_TREC_ID_VALUE);
            OUTPUT_DOC.put(TITLE_KEY,         TITLE_VALUE);
            OUTPUT_DOC.put(META_DESC_KEY,     META_DESC_VALUE);
            OUTPUT_DOC.put(META_KEYWORDS_KEY, META_KEYWORDS_VALUE);
            OUTPUT_DOC.put(BODY_KEY,          BODY_VALUE);
            OUTPUT_DOC.put(BODY_LENGTH_KEY,   BODY_LENGTH_VALUE);
            context.write(WARC_TREC_ID_VALUE, OUTPUT_DOC);
        } catch (final StackOverflowError ex) {
            // HTML too deeply nested
            if (null == bodySource) {
                LOG.warn(String.format("Document %s with deep nesting level skipped", docId));
            } else {
                LOG.warn(String.format("Document %s with approximate nesting level of %d skipped", docId, bodySource.getMaxDepthIndicator()));
            }
            tooDeepCounter.increment(1);
        }
    }

    /**
     * Configure and return a Renderer for a Source document.
     *
     * @param source Jericho Source object
     * @return configured Renderer
     */
    /*private Renderer getHTMLToTextRenderer(final Source source)
    {
        return source.
                getRenderer().
                setIncludeHyperlinkURLs(false).
                setTableCellSeparator("").
                setHRLineLength(0).
                setMaxLineLength(600).
                setNewLine("\n").
                setBlockIndentSize(0).
                setListIndentSize(0);
    }*/

    /**
     * Get title from source document.
     *
     * @param source Jericho Source object
     * @param maxLength maximum length of content to return, content that is longer will be truncated
     * @return document title
     */
    private String getDocTitle(final net.htmlparser.jericho.Source source, final int maxLength)
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
    private String getMetaTagContents(final net.htmlparser.jericho.Source source, final String type, final String what, final int maxLength)
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