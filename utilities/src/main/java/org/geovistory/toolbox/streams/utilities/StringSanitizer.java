package org.geovistory.toolbox.streams.utilities;

/**
 * Utility class for string manipulation
 */
public class StringSanitizer {

    /**
     * @param s the String to escape
     * @return escaped String
     */
    public static String escapeBackslashAndDoubleQuote(String s) {
        return s.replaceAll("[\\\\\"]", "\\\\$0");
    }
}
