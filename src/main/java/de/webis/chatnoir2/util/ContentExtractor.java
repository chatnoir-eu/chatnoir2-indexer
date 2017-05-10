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

import de.aitools.aq.web.extractor.PotthastJerichoExtractor;
import org.jsoup.Jsoup;
import org.jsoup.helper.StringUtil;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Extractor for plain text contents of HTML documents.
 */
public class ContentExtractor
{
    /**
     * Extract contents.
     *
     * @param html HTML source text
     * @return extracted plain text (may be empty)
     */
    public static String extract(String html)
    {
        if (null == html || html.trim().isEmpty()) {
            return "";
        }

        PotthastJerichoExtractor extractor = new PotthastJerichoExtractor();
        extractor.setMinParagraphLengthInCharacters(70);
        extractor.setTimeoutInSeconds(20);
        try {
            return extractor.extractSentences(html).stream().collect(Collectors.joining(" "));
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * Extract all textual contents from HTML, not only main article content.
     *
     * @param html HTML source text
     * @param normalizeWhitespace whether to normalize (collapse) white space
     * @return extracted plain text, may be empty
     */
    public static String extractEverything(String html, boolean normalizeWhitespace)
    {
        if (null == html || html.trim().isEmpty()) {
            return "";
        }

        try {
            String plainText = "";
            Elements body = Jsoup.parse(html).getElementsByTag("body");
            if (body.size() > 0) {
                plainText = body.get(0).text().trim();
                if (normalizeWhitespace) {
                    plainText = StringUtil.normaliseWhitespace(plainText);
                }
            }
            return plainText;
        } catch (Exception e) {
            return html.trim();
        }
    }

    /**
     * Extract HTML headings from source text up to a given maximum level.
     *
     * @param html HTML source text
     * @param maxLevel maximum heading level to extract (1-6)
     * @return extracted headings, separated by newlines
     */
    public static String extractHeadings(String html, int maxLevel)
    {
        if (null == html || html.trim().isEmpty()) {
            return "";
        }

        try {
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
                    headings.append(StringUtil.normaliseWhitespace(e.text().trim()));
                }
            }

            return headings.toString();
        } catch (Exception e) {
            return "";
        }
    }
}
