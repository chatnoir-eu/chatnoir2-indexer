/*
 * Elasticsearch Indexer for WARC JSON Mapfiles using Hadoop MapReduce.
 * Copyright (C) 2014-2015 Janek Bevendorff <janek.bevendorff@uni-weimar.de>
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

import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLConnection;

/**
 * Language detection helper class.
 *
 * @author Janek Bevendorff
 */
public class LangDetector
{
    // private final LanguageDetector mLanguageDetector;
    // private final TextObjectFactory mShortTextFactory;
    // private final TextObjectFactory mLongTextFactory;

    private final String mLangDetectHost;

    /**
     * Create language detector for given context.
     * If indexer.langdetect.host is set in the configuration, that host will be used.
     * Otherwise the default of localhost:9200 is assumed. The given host must be running an Elasticsearch
     * instance with installed langdetect plugin.
     *
     * @param context Mapper context containing configuration
     */
    public LangDetector(Mapper.Context context)
    {
        /*final List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();
        mLanguageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard()).
                withProfiles(languageProfiles).build();
        mShortTextFactory = CommonTextObjectFactories.forDetectingShortCleanText();
        mLongTextFactory = CommonTextObjectFactories.forDetectingOnLargeText();*/
        String host = context.getConfiguration().get("indexer.langdetect.host");
        if (null == host || host.isEmpty()) {
            host = "localhost:9200";
        }
        mLangDetectHost = host;
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
        /*final TextObject textObject;
        if (300 > renderedBody.length()) {
            textObject = SHORT_TEXT_FACTORY.forText(renderedBody);
        } else {
            textObject = LONG_TEXT_FACTORY.forText(renderedBody);
        }
        final Optional<LdLocale> langOpt = LANGUAGE_DETECTOR.detect(textObject);
        if (langOpt.isPresent()) {
            lang = langOpt.get().getLanguage().substring(0, 2).toLowerCase();
        } else {
            LOG.warn("Language detection failed for document " + key + ", falling back to " + lang);
        }*/

        try {
            final URL url            = new URL("http://" + mLangDetectHost + "/_langdetect");
            final URLConnection conn = url.openConnection();
            conn.setDoOutput(true);
            final PrintStream ps = new PrintStream(conn.getOutputStream());
            ps.print(str);
            ps.close();

            BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            final StringBuilder strBuilder = new StringBuilder();
            while (null != (line = br.readLine())) {
                strBuilder.append(line);
            }
            br.close();
            final JSONObject json = new JSONObject(strBuilder.toString());
            return json.getJSONArray("languages").getJSONObject(0).
                    getString("language").substring(0, 2).toLowerCase();
        } catch (JSONException | IOException e) {
            throw new IOException("Language detection failed: " + e.getMessage());
        }
    }
}
