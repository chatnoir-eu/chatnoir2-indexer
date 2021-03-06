/*
 * ChatNoir Indexing Backend.
 * Copyright (C) 2014-2017 Janek Bevendorff, Webis Group
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package de.webis.chatnoir2.indexer.util;

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
