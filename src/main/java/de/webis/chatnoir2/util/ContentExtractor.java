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

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import de.l3s.boilerpipe.extractors.DefaultExtractor;
import de.l3s.boilerpipe.extractors.KeepEverythingExtractor;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.util.ArrayList;
import java.util.List;

/**
 * Extractor for plain text contents of HTML documents.
 */
public class ContentExtractor
{
    /**
     * Extract contents.
     *
     * @param html HTML source text
     * @return extracted plain text, null if extraction failed
     */
    public static String extract(String html)
    {
        String plainText = null;
        try {
            plainText = ArticleExtractor.getInstance().getText(html);
            if (plainText.length() < 600) {
                plainText = DefaultExtractor.getInstance().getText(html);
            }
        } catch (BoilerpipeProcessingException e) {
            e.printStackTrace();
        }

        return plainText;
    }

    /**
     * Extract all text contents from HTML, not only main article content.
     *
     * @param html HTML source text
     * @return extracted plain text, null if extraction failed
     */
    public static String extractEverything(String html)
    {
        String plainText = null;
        try {
            plainText = KeepEverythingExtractor.INSTANCE.getText(html);
        } catch (BoilerpipeProcessingException e) {
            e.printStackTrace();
        }

        return plainText;
    }

    /**
     * Extract HTML headings from source text up to a given maximum level.
     *
     * @param html HTML source text
     * @param maxLevel maximum heading level to extract (1-6)
     * @return extracted headings, separated by newlines
     */
    public static String extractHeadings(String html, int maxLevel) {
        Document doc = Jsoup.parse(html);
        StringBuilder headings = new StringBuilder();

        if (maxLevel < 1) {
            maxLevel = 1;
        } else if (maxLevel > 6) {
            maxLevel = 6;
        }

        for (int i = 1; i <= maxLevel; ++i) {
            List<Element> elements = doc.select(String.format("h%d", i));
            for (Element e : elements) {
                headings.append(e.text().replaceAll("[\\n\\r\\s]+", " "));
            }
        }

        return headings.toString();
    }
}
