package org.geovistory.toolbox.streams.lib;

public class Utils {
    /**
     * tsPrefixed
     *
     * @param name the string to be prefixed
     * @return string with format {prefix}.{name}
     */
    public static String tsPrefixed(String name) {
        return AppConfig.INSTANCE.getOutputTopicPrefix() + "." + name;
    }

    /**
     * dbPrefixed
     *
     * @param name the string to be prefixed
     * @return string with format {prefix}.{name}
     */
    public static String dbPrefixed(String name) {
        return AppConfig.INSTANCE.getInputTopicPrefix() + "." + name;
    }

    @SafeVarargs
    public static <T> T coalesce(T... items) {
        for (T i : items) if (i != null) return i;
        return null;
    }
}
