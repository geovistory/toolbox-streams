package org.geovistory.toolbox.streams.lib;


import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.JulianFields;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class TimeUtils {

    private static final int secondsPerDay = 86400; // 24*60*60

    public static YearMonthDay getYearMonthDay(int julianDay, CalendarType calendar) {

        if (calendar == CalendarType.julian) return getYearMonthDayJulianCal(julianDay);
        return getYearMonthDayGregorianCal(julianDay);
    }


    private static YearMonthDay getYearMonthDayJulianCal(int julianDay) {


        var localDate = LocalDate.MIN.with(JulianFields.JULIAN_DAY, julianDay);

        // To obtain a pure Julian calendar, set the change date to Date(Long.MAX_VALUE).
        GregorianCalendar julianCalendar = GregorianCalendar.from(localDate.atStartOfDay(ZoneId.of("Europe/Paris")));
        julianCalendar.setGregorianChange(new Date(Long.MAX_VALUE));

        var year = julianCalendar.get(Calendar.YEAR);
        var month = julianCalendar.get(Calendar.MONTH) + 1;
        var day = julianCalendar.get(Calendar.DAY_OF_MONTH);

        return new YearMonthDay(year, month, day);

    }


    private static YearMonthDay getYearMonthDayGregorianCal(int julianDay) {
        var localDate = LocalDate.MIN.with(JulianFields.JULIAN_DAY, julianDay);
        return new YearMonthDay(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
    }

    public static String getIso8601String(int julianDay) {
        var localDate = LocalDate.MIN.with(JulianFields.JULIAN_DAY, julianDay);
        return localDate.toString() + "T00:00:00Z";
    }


    public static int getJulianDayPlusDuration(int julianDay, String duration) {
        return getJulianDayPlus(julianDay, 1, getChronoUnitFromDuration(duration));
    }

    private static ChronoUnit getChronoUnitFromDuration(String duration) {
        return switch (duration) {
            case "1 year" -> ChronoUnit.YEARS;
            case "1 month" -> ChronoUnit.MONTHS;
            case "1 day" -> ChronoUnit.DAYS;
            default -> ChronoUnit.DAYS;
        };
    }

    private static int getJulianDayPlus(int julianDay, int amountToAdd, ChronoUnit unit) {

        var localDate = LocalDate.MIN.with(JulianFields.JULIAN_DAY, julianDay);

        var added = localDate.plus(amountToAdd, unit);

        return Math.toIntExact(added.getLong(JulianFields.JULIAN_DAY));
    }


    public static long getJulianSecond(long julianDay) {
        return julianDay * secondsPerDay;
    }

    public record YearMonthDay(int year, int month, int day) {
        @Override
        public String toString() {
            return String.format("%04d", year) + "-" + String.format("%02d", month) + "-" + String.format("%02d", day);
        }

    }

    public enum CalendarType {
        gregorian, julian
    }

}
