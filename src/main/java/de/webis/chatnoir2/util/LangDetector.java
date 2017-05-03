/*
 * Elasticsearch Indexer for WARC JSON Mapfiles using Hadoop MapReduce.
 * Copyright (C) 2014-2017 Janek Bevendorff <janek.bevendorff@uni-weimar.de>
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

package de.webis.chatnoir2.util;

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

import java.io.IOException;
import java.util.List;

/**
 * Language detection helper class.
 *
 * @author Janek Bevendorff
 */
public class LangDetector
{
    private final LanguageDetector mLanguageDetector;

    /**
     * Create language detector for given context.
     *
     * @throws IOException if failed to load language resources
     */
    public LangDetector() throws IOException
    {
        List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();
        mLanguageDetector = LanguageDetectorBuilder
                .create(NgramExtractors.standard())
                .withProfiles(languageProfiles)
                .build();
    }

    /**
     * Detect language of a string.
     *
     * @param str the string whose language to detect
     * @return detected ISO language code
     * @throws IOException if language detection fails
     */
    public String detect(final String str) throws IOException
    {
        TextObjectFactory textObjectFactory;
        if (str.length() > 400) {
            textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();
        } else {
            textObjectFactory = CommonTextObjectFactories.forDetectingShortCleanText();
        }
        TextObject textObject = textObjectFactory.forText(str);
        Optional<LdLocale> language = mLanguageDetector.detect(textObject);

        if (!language.isPresent()) {
            throw new IOException("Language detection failed!");
        }

        return language.get().getLanguage();
    }
}
