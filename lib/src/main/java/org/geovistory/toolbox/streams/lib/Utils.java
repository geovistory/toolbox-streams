package org.geovistory.toolbox.streams.lib;

public class Utils {
    /**
     * prefixed
     *
     * @param name the string to be prefixed
     * @return string with format {prefix}.{name}
     */
    public static String prefixed(String name) {
        return AppConfig.INSTANCE.getTopicPrefix() + "." + name;
    }

    @SafeVarargs
    public static <T> T coalesce(T... items) {
        for (T i : items) if (i != null) return i;
        return null;
    }
}
