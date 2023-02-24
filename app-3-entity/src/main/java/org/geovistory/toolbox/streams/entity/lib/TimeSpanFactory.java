package org.geovistory.toolbox.streams.entity.lib;

import org.geovistory.toolbox.streams.avro.*;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.time.temporal.JulianFields;

public class TimeSpanFactory {
    public static String[] keys = new String[]{"71_out", "72_out", "150_out", "151_out", "152_out", "153_out"};
    private static final int secondsPerDay = 86400; // 24*60*60


    /**
     * creates TimeSpanValue for an entity
     *
     * @param value, the ProjectEntityTopStatementsValue of the entity
     * @return TimeSpanValue:
     * - firstSecond: Earliest julianDay found in all timePrimitives, multiplied by 12 * 60 * 60
     * - lastSecond: Latest second calculated for all TimePrimitives by adding the duration to the julianDay and multiplied by 12 * 60 * 60
     * - timeSpan: An aggregation of six properties: begin of begin, end of begin, begin of end, end of end, at some time within, ongoing throughout
     */
    public static TimeSpanValue createTimeSpan(ProjectEntityTopStatementsValue value) {
        return new Parser().getTimeSpan(value);
    }

    /**
     * creates TimeSpanValue for an entity
     *
     * @param value, the CommunityEntityTopStatementsValue of the entity
     * @return TimeSpanValue:
     * - firstSecond: Earliest julianDay found in all timePrimitives, multiplied by 12 * 60 * 60
     * - lastSecond: Latest second calculated for all TimePrimitives by adding the duration to the julianDay and multiplied by 12 * 60 * 60
     * - timeSpan: An aggregation of six properties: begin of begin, end of begin, begin of end, end of end, at some time within, ongoing throughout
     */
    public static TimeSpanValue createTimeSpan(CommunityEntityTopStatementsValue value) {
        return new Parser().getTimeSpan(value);
    }

    private static class Parser {
        TimeSpan.Builder timeSpan = TimeSpan.newBuilder();
        long firstSecond = Long.MAX_VALUE;
        long lastSecond = Long.MIN_VALUE;
        boolean isEmpty = true;

        public TimeSpanValue getTimeSpan(ProjectEntityTopStatementsValue value) {
            for (var key : keys) {
                var temporalStatements = value.getMap().get(key);
                if (temporalStatements != null) {
                    var statements = temporalStatements.getStatements();
                    if (statements != null && statements.size() > 0) {
                        for (var s : statements) {
                            var tp = extractTimePrimitive(s);
                            processTimePrimitive(key, tp);
                        }
                    }
                }
            }
            return getTimeSpanValue();
        }
        public TimeSpanValue getTimeSpan(CommunityEntityTopStatementsValue value) {
            for (var key : keys) {
                var temporalStatements = value.getMap().get(key);
                if (temporalStatements != null) {
                    var statements = temporalStatements.getStatements();
                    if (statements != null && statements.size() > 0) {
                        for (var s : statements) {
                            var tp = extractTimePrimitive(s);
                            processTimePrimitive(key, tp);
                        }
                    }
                }
            }
            return getTimeSpanValue();
        }

        public static TimePrimitive extractTimePrimitive(CommunityStatementValue value) {
            var a = value.getStatement();
            if (a == null) return null;

            var b = a.getObject();
            if (b == null) return null;

            return b.getTimePrimitive();
        }

        public static TimePrimitive extractTimePrimitive(ProjectStatementValue value) {
            var a = value.getStatement();
            if (a == null) return null;

            var b = a.getObject();
            if (b == null) return null;

            return b.getTimePrimitive();
        }

        private TimeSpanValue getTimeSpanValue() {
            if (isEmpty) return null;

            return TimeSpanValue.newBuilder()
                    .setFirstSecond(firstSecond)
                    .setLastSecond(lastSecond)
                    .setTimeSpan(timeSpan.build())
                    .build();
        }

        private void processTimePrimitive(String key, TimePrimitive tp) {
            if (tp != null) {
                isEmpty = false;
                // create NewTimePrimitive
                var ntp = createNewTimePrimitive(tp);

                // set NewTimePrimitive in TimeSpan
                setTimePrimitiveInTimeSpan(timeSpan, key, ntp);

                // create the first second
                long fs = createFirstSecond(tp);
                // set the first second
                if (firstSecond > fs) firstSecond = fs;

                // create the last second
                long ls = createLastSecond(tp);
                // set the last second
                if (lastSecond < ls) lastSecond = ls;


            }
        }
    }




    public static NewTimePrimitive createNewTimePrimitive(TimePrimitive value) {
        return NewTimePrimitive.newBuilder()
                .setCalendar(value.getCalendar())
                .setDuration(value.getDuration())
                .setJulianDay(value.getJulianDay())
                .build();
    }

    public static void setTimePrimitiveInTimeSpan(TimeSpan.Builder timeSpanBuilder, String key, NewTimePrimitive ntp) {
        switch (key) {
            case "71_out" -> timeSpanBuilder.setP81(ntp);
            case "72_out" -> timeSpanBuilder.setP82(ntp);
            case "150_out" -> timeSpanBuilder.setP81a(ntp);
            case "151_out" -> timeSpanBuilder.setP81b(ntp);
            case "152_out" -> timeSpanBuilder.setP82a(ntp);
            case "153_out" -> timeSpanBuilder.setP82b(ntp);
        }
    }

    public static long createFirstSecond(TimePrimitive value) {
        return (long) value.getJulianDay() * secondsPerDay;
    }

    public static long createLastSecond(TimePrimitive value) {

        var localDate = LocalDate.MIN.with(JulianFields.JULIAN_DAY, value.getJulianDay());
        var date = switch (value.getDuration()) {
            case "1 year" -> localDate.plus(1, ChronoUnit.YEARS);
            case "1 month" -> localDate.plus(1, ChronoUnit.MONTHS);
            case "1 day" -> localDate.plus(1, ChronoUnit.DAYS);
            default -> LocalDate.MIN.with(JulianFields.JULIAN_DAY, value.getJulianDay());

        /*
        var calendar = new GregorianCalendar();
        // from docs:
        // To obtain a pure Julian calendar, set the change date to Date(Long.MAX_VALUE).
        calendar.setGregorianChange(new Date(Long.MAX_VALUE));
        GregorianCalendar gc = GregorianCalendar.from(date.atStartOfDay(ZoneId.of("Europe/Paris")));
        gc.isLeapYear();
        */
        };

        long newJulianDay = date.getLong(JulianFields.JULIAN_DAY);

        return newJulianDay * secondsPerDay - 1;
    }
}
