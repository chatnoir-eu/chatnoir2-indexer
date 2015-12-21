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

package org.clueweb.util;

import org.jsoup.helper.StringUtil;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.NodeTraversor;
import org.jsoup.select.NodeVisitor;

/**
 * Create very basic text rendering of a HTML document using JSoup.
 * Based on HtmlToPlainText class by Jonathan Hedley &lt;jonathan@hedley.net&gt;
 *
 * @author Janek Bevendorff
 * @author Jonathan Hedley
 */
public class HtmlToPlainText
{
    /**
     * Format an Element to plain-text
     * @param element the root element to format
     * @return formatted text
     */
    public String getPlainText(final Element element)
    {
        final FormattingVisitor formatter = new FormattingVisitor();
        final NodeTraversor traversor     = new NodeTraversor(formatter);
        traversor.traverse(element);

        return formatter.toString();
    }

    /**
     * Formatting rules for breadth-first DOM traversal.
     */
    private class FormattingVisitor implements NodeVisitor
    {
        private static final int mMaxWidth = 80;
        private int mWidth                 = 0;
        private final StringBuilder mAccum = new StringBuilder();

        /**
         * Hit when the node is first seen.
         */
        public void head(final Node node, int depth)
        {
            final String name = node.nodeName();
            if (node instanceof TextNode)
                append(((TextNode) node).text()); // TextNodes carry all user-readable text in the DOM.
            else if (name.equals("li"))
                append("\n   ");
        }

        /**
         * Hit when all of the node's children (if any) have been visited.
         */
        public void tail(final Node node, final int depth)
        {
            final String name = node.nodeName();
            if (name.equals("br"))
                append("\n");
            else if (StringUtil.in(name, "p", "h1", "h2", "h3", "h4", "h5"))
                append("\n");
        }

        /**
         * Append text to the string builder with a simple word wrap method.
         */
        private void append(final String text)
        {
            if (text.startsWith("\n")) {
                // reset counter if starts with a newline. only from formats above, not in natural text
                mWidth = 0;
            }
            if (text.equals(" ") && (mAccum.length() == 0 ||
                    StringUtil.in(mAccum.substring(mAccum.length() - 1), " ", "\n"))) {
                // don't accumulate long runs of empty spaces
                return;
            }

            if (text.length() + mWidth > mMaxWidth) {
                // won't fit, needs to wrap
                String words[] = text.split("\\s+");
                for (int i = 0; i < words.length; i++) {
                    String word = words[i];
                    boolean last = i == words.length - 1;
                    if (!last) {
                        // insert a space if not the last word
                        word = word + " ";
                    }
                    if (word.length() + mWidth > mMaxWidth) {
                        // wrap and reset counter
                        mAccum.append("\n").append(word);
                        mWidth = word.length();
                    } else {
                        mAccum.append(word);
                        mWidth += word.length();
                    }
                }
            } else {
                // fits as is, without wrapping
                mAccum.append(text);
                mWidth += text.length();
            }
        }

        public String toString()
        {
            return mAccum.toString();
        }
    }
}
