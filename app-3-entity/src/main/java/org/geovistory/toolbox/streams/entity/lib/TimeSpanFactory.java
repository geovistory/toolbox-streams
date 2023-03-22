package org.geovistory.toolbox.streams.entity.lib;

import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.TimeUtils;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.time.temporal.JulianFields;

public class TimeSpanFactory {

    /**
     * creates TimeSpanValue for an entity
     *
     * @param value, the TopTimePrimitivesMap of the entity
     * @return TimeSpanValue:
     * - firstSecond: Earliest julianDay found in all timePrimitives, multiplied by 12 * 60 * 60
     * - lastSecond: Latest second calculated for all TimePrimitives by adding the duration to the julianDay and multiplied by 12 * 60 * 60
     * - timeSpan: An aggregation of six properties: begin of begin, end of begin, begin of end, end of end, at some time within, ongoing throughout
     */
    public static TimeSpanValue createTimeSpan(TopTimePrimitivesMap value) {
        return new Parser().getTimeSpan(value);
    }


    private static class Parser {
        TimeSpan.Builder timeSpan = TimeSpan.newBuilder();
        long firstSecond = Long.MAX_VALUE;
        long lastSecond = Long.MIN_VALUE;
        boolean isEmpty = true;

        public TimeSpanValue getTimeSpan(TopTimePrimitivesMap value) {
            for (var record : value.getMap().entrySet()) {
                var f = record.getValue();
                for (var tp : f.getTimePrimitives()) {
                    processTimePrimitive(f.getPropertyId(), tp);
                }

            }
            return getTimeSpanValue();
        }


        private TimeSpanValue getTimeSpanValue() {
            if (isEmpty) return null;

            return TimeSpanValue.newBuilder()
                    .setFirstSecond(firstSecond)
                    .setLastSecond(lastSecond)
                    .setTimeSpan(timeSpan.build())
                    .build();
        }

        private void processTimePrimitive(int key, TimePrimitive tp) {
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

    public static void setTimePrimitiveInTimeSpan(TimeSpan.Builder timeSpanBuilder, int key, NewTimePrimitive ntp) {
        switch (key) {
            case 71 -> timeSpanBuilder.setP81(ntp);
            case 72 -> timeSpanBuilder.setP82(ntp);
            case 150 -> timeSpanBuilder.setP81a(ntp);
            case 151 -> timeSpanBuilder.setP81b(ntp);
            case 152 -> timeSpanBuilder.setP82a(ntp);
            case 153 -> timeSpanBuilder.setP82b(ntp);
        }
    }

    public static long createFirstSecond(TimePrimitive value) {
        return TimeUtils.getJulianSecond(value.getJulianDay());
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

        return TimeUtils.getJulianSecond(newJulianDay) - 1;
    }

    public static boolean isTimeProperty(int p) {
        return p == 71 || p == 72 || p == 150 || p == 151 || p == 152 || p == 153;
    }
}
