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

package de.webis.chatnoir2.chatnoir2_indexer.util;

import de.aitools.ie.languagedetection.LanguageDetector;

import java.io.IOException;
import java.util.Locale;

/**
 * Language detection helper class.
 *
 * @author Janek Bevendorff
 */
public class LangDetector
{

    private final LanguageDetector mDetector = new LanguageDetector();

    /**
     * Create language detector for given context.
     *
     * @throws IOException if failed to load language resources
     */
    public LangDetector() throws IOException
    {
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
        Locale language = mDetector.detect(str);
        return language.getLanguage();
    }
}
