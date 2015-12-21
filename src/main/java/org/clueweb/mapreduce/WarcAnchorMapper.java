package org.clueweb.mapreduce;

import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Mapper class for link anchor texts.
 *
 * @author Janek Bevendorff
 */
public class WarcAnchorMapper extends Mapper<LongWritable, Text, Text, MapWritable> implements WarcMapReduceBase
{
    /**
     * Cut anchor texts after MAX_LENGTH characters.
     */
    public static final int MAX_LENGTH = 400;

    protected static Pattern REGEX;

    protected static LanguageDetector LANGUAGE_DETECTOR   = null;
    protected static TextObjectFactory SHORT_TEXT_FACTORY = null;

    @Override
    protected void setup(final Context context) throws IOException
    {
        REGEX = Pattern.compile("(clueweb\\d{2}-\\w{2}\\d{4}-\\d{2}-\\d{5})\\s+(.*)");

        if (null == LANGUAGE_DETECTOR) {
            final List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();
            LANGUAGE_DETECTOR = LanguageDetectorBuilder.create(NgramExtractors.standard()).
                    withProfiles(languageProfiles).build();
            SHORT_TEXT_FACTORY = CommonTextObjectFactories.forDetectingShortCleanText();
        }
    }

    @Override
    public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
    {
        final String strValue = value.toString();
        final Matcher m = REGEX.matcher(strValue);

        if (m.matches() && null != m.group(1) && null != m.group(2)) {
            final String recordId = m.group(1);
            String anchorValue    = m.group(2);
            if (MAX_LENGTH < anchorValue.length()) {
                anchorValue = anchorValue.substring(0, MAX_LENGTH);
            }

            // language detection
            String lang = "en";
            final TextObject textObject = SHORT_TEXT_FACTORY.forText(anchorValue);
            final Optional<LdLocale> langOpt = LANGUAGE_DETECTOR.detect(textObject);
            if (langOpt.isPresent()) {
                lang = langOpt.get().getLanguage().substring(0, 2).toLowerCase();
            } else {
                LOG.warn("Language detection failed for anchor text for document " + key + ", falling back to " + lang);
            }

            MAPREDUCE_KEY.set(recordId);
            ANCHOR_TEXT_VALUE.set(anchorValue);

            OUTPUT_MAP_DOC.clear();
            OUTPUT_MAP_DOC.put(new Text(ANCHOR_TEXTS_BASE_KEY + lang), ANCHOR_TEXT_VALUE);
            context.write(MAPREDUCE_KEY, OUTPUT_MAP_DOC);
        }
    }
}