package org.geovistory.toolbox.streams.lib;

import org.apache.commons.text.StringEscapeUtils;

/**
 * Utility class for string manipulation
 */
public class StringSanitizer {

    /**
     * Escape special characters in Java String.
     * Useful to create turtle literals.
     *
     * @param s the String to escape
     * @return escaped String
     */
    public static String escapeJava(String s) {
        return StringEscapeUtils.escapeJava(s);
    }
}
