package org.geovistory.toolbox.streams.lib;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Utils {
    /**
     * tsPrefixed
     *
     * @param name the string to be prefixed
     * @return string with format {prefix}_{name}
     */
    public static String tsPrefixed(String name) {
        var topicName = AppConfig.INSTANCE.getOutputTopicPrefix() + "_" + name;
        return topicName.replace('.', '_');
    }

    public static String prefixedOut(String prefix, String name) {
        var topicName = prefix + "_" + name;
        return topicName.replace('.', '_');
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

    public static String prefixedIn(String prefix, String name) {
        return prefix + "." + name;
    }

    @SafeVarargs
    public static <T> T coalesce(T... items) {
        for (T i : items) if (i != null) return i;
        return null;
    }

    public static Boolean includesTrue(Boolean b1, Boolean b2) {
        Boolean[] array = {b1, b2};
        Boolean value = true;
        boolean found = false;
        for (Boolean element : array) {
            if (value.equals(element)) {
                found = true;
                break;
            }
        }
        return found;
    }

    /**
     * Checks if given string is "true".
     *
     * @param s string or null
     * @return if s=="true" returns true, else false
     */
    public static Boolean stringIsEqualTrue(String s) {
        return Objects.equals(s, "true");
    }

    public static Boolean booleanIsNotEqualTrue(Boolean s) {
        if (s == null) return true;
        return !s;
    }

    public static Boolean booleanIsEqualTrue(Boolean s) {
        if (s == null) return false;
        return s;
    }


    /**
     * Returns the language Id of a Geovistory Language for the given language code
     *
     * @param isoLang for example 'fr' or 'de'
     * @return id of the language in the Geovistory database
     */
    public static Integer isoLangToGeoId(String isoLang) {
        return languageMap().get(isoLang.trim());

    }

    /**
     * Shorten a string to a maximum length
     *
     * @param inputString string to shorten
     * @param maxLength   the maximum length of the string
     * @return if inputString is longer than maxLength, the first {maxLength} characters of inputString,
     * else the inputString
     * if inputString is null, returns null
     */
    public static String shorten(String inputString, int maxLength) {
        if (inputString == null) return null;
        if (inputString.length() > maxLength) return inputString.substring(0, maxLength);
        else return inputString;
    }

    public static Instant InstantFromIso(String s) {
        try {

            TemporalAccessor ta = DateTimeFormatter.ISO_INSTANT.parse(s);
            return Instant.from(ta);
        } catch (NullPointerException | IllegalArgumentException e) {
            return null;
        }
    }

    public static Date DateFromIso(String s) {
        try {

            TemporalAccessor ta = DateTimeFormatter.ISO_INSTANT.parse(s);
            Instant instant = Instant.from(ta);
            return Date.from(instant);
        } catch (NullPointerException | IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * parse an integer from a string like "i123123" or "d232"
     *
     * @param value1 input string
     * @return parsed integer or, in case of exception, 0
     */
    public static int parseStringId(String value1) {
        try {
            return Integer.parseInt(value1.substring(1));
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private static Map<String, Integer> languageMap() {
        Map<String, Integer> map = new HashMap<>();
        map.put("aa", 17082);
        map.put("ab", 17099);
        map.put("af", 17184);
        map.put("ak", 17260);
        map.put("am", 17314);
        map.put("ar", 17413);
        map.put("an", 17418);
        map.put("as", 17448);
        map.put("av", 17508);
        map.put("ae", 17511);
        map.put("ay", 17558);
        map.put("az", 17572);
        map.put("ba", 17588);
        map.put("bm", 17590);
        map.put("be", 17689);
        map.put("bn", 17691);
        map.put("bi", 17793);
        map.put("bo", 17925);
        map.put("bs", 17940);
        map.put("br", 18000);
        map.put("bg", 18080);
        map.put("ca", 18235);
        map.put("cs", 18290);
        map.put("ch", 18300);
        map.put("ce", 18304);
        map.put("cu", 18318);
        map.put("cv", 18319);
        map.put("kw", 18419);
        map.put("co", 18420);
        map.put("cr", 18443);
        map.put("cy", 18529);
        map.put("da", 18547);
        map.put("de", 18605);
        map.put("dv", 18660);
        map.put("dz", 18826);
        map.put("el", 18865);
        map.put("en", 18889);
        map.put("eo", 18903);
        map.put("et", 18925);
        map.put("eu", 18939);
        map.put("ee", 18943);
        map.put("fo", 18962);
        map.put("fa", 18965);
        map.put("fj", 18979);
        map.put("fi", 18981);
        map.put("fr", 19008);
        map.put("fy", 19019);
        map.put("ff", 19031);
        map.put("gd", 19192);
        map.put("ga", 19195);
        map.put("gl", 19196);
        map.put("gv", 19205);
        map.put("gn", 19282);
        map.put("gu", 19314);
        map.put("ht", 19393);
        map.put("ha", 19394);
        map.put("sh", 19404);
        map.put("he", 19412);
        map.put("hz", 19418);
        map.put("hi", 19434);
        map.put("ho", 19465);
        map.put("hr", 19516);
        map.put("hu", 19542);
        map.put("hy", 19564);
        map.put("ig", 19576);
        map.put("io", 19590);
        map.put("ii", 19616);
        map.put("iu", 19632);
        map.put("ie", 19639);
        map.put("ia", 19657);
        map.put("id", 19659);
        map.put("ik", 19675);
        map.put("is", 19696);
        map.put("it", 19703);
        map.put("jv", 19752);
        map.put("ja", 19839);
        map.put("kl", 19883);
        map.put("kn", 19885);
        map.put("ks", 19889);
        map.put("ka", 19890);
        map.put("kr", 19891);
        map.put("kk", 19896);
        map.put("km", 20056);
        map.put("ki", 20080);
        map.put("rw", 20083);
        map.put("ky", 20087);
        map.put("kv", 20234);
        map.put("kg", 20235);
        map.put("ko", 20239);
        map.put("kj", 20372);
        map.put("ku", 20389);
        map.put("lo", 20535);
        map.put("la", 20540);
        map.put("lv", 20542);
        map.put("li", 20656);
        map.put("ln", 20657);
        map.put("lt", 20663);
        map.put("lb", 20817);
        map.put("lu", 20819);
        map.put("lg", 20824);
        map.put("mh", 20869);
        map.put("ml", 20873);
        map.put("mr", 20877);
        map.put("mk", 21112);
        map.put("mg", 21139);
        map.put("mt", 21152);
        map.put("mn", 21217);
        map.put("mi", 21288);
        map.put("ms", 21306);
        map.put("my", 21453);
        map.put("na", 21518);
        map.put("nv", 21519);
        map.put("nr", 21534);
        map.put("nd", 21573);
        map.put("ng", 21583);
        map.put("ne", 21609);
        map.put("nl", 21740);
        map.put("nn", 21795);
        map.put("nb", 21807);
        map.put("no", 21822);
        map.put("ny", 21957);
        map.put("oc", 22005);
        map.put("oj", 22028);
        map.put("or", 22107);
        map.put("om", 22108);
        map.put("os", 22125);
        map.put("pa", 22170);
        map.put("pi", 22315);
        map.put("pl", 22383);
        map.put("pt", 22389);
        map.put("ps", 22479);
        map.put("qu", 22507);
        map.put("rm", 22670);
        map.put("ro", 22673);
        map.put("rn", 22701);
        map.put("ru", 22705);
        map.put("sg", 22727);
        map.put("sa", 22732);
        map.put("si", 22893);
        map.put("sk", 22953);
        map.put("sl", 22963);
        map.put("se", 22972);
        map.put("sm", 22981);
        map.put("sn", 22993);
        map.put("sd", 22996);
        map.put("so", 23029);
        map.put("st", 23035);
        map.put("es", 23042);
        map.put("sq", 23065);
        map.put("sc", 23078);
        map.put("sr", 23089);
        map.put("ss", 23121);
        map.put("su", 23156);
        map.put("sw", 23174);
        map.put("sv", 23177);
        map.put("ty", 23243);
        map.put("ta", 23247);
        map.put("tt", 23254);
        map.put("te", 23341);
        map.put("tg", 23369);
        map.put("tl", 23370);
        map.put("th", 23384);
        map.put("ti", 23418);
        map.put("to", 23537);
        map.put("tn", 23619);
        map.put("ts", 23620);
        map.put("tk", 23667);
        map.put("tr", 23673);
        map.put("tw", 23701);
        map.put("ug", 23782);
        map.put("uk", 23792);
        map.put("ur", 23838);
        map.put("uz", 23878);
        map.put("ve", 23904);
        map.put("vi", 23912);
        map.put("vo", 23958);
        map.put("wa", 24081);
        map.put("wo", 24127);
        map.put("xh", 24278);
        map.put("yi", 24572);
        map.put("yo", 24651);
        map.put("za", 24776);
        map.put("zh", 24781);
        map.put("zu", 24901);
        return map;
    }

}
