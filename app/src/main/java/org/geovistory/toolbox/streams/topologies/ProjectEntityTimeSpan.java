package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.app.RegisterOutputTopic;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.time.temporal.JulianFields;
import java.util.LinkedList;
import java.util.List;


public class ProjectEntityTimeSpan {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {

        var register = new RegisterOutputTopic(builder);

        var apiClassTable = register.projectEntityTopStatementsStream();

        return addProcessors(builder, apiClassTable).builder().build();
    }

    public static int secondsPerDay = 86400;// = 60 * 60 * 24 = number of seconds per day
    public static String[] keys = new String[]{"71_out", "72_out", "150_out", "151_out", "152_out", "153_out"};

    public static ProjectEntityTimeSpanReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<ProjectEntityKey, ProjectEntityTopStatementsValue> projectEntityTopStatementsValueStream
    ) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)

        var stream = projectEntityTopStatementsValueStream.flatMapValues((readOnlyKey, value) -> {
            List<TimeSpanValue> result = new LinkedList<>();
            var tspv = createTimeSpan(value);
            if (tspv != null) result.add(tspv);
            return result;
        });



        /* SINK PROCESSORS */
        stream.to(output.TOPICS.project_entity_time_span,
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.TimeSpanValue()));

        return new ProjectEntityTimeSpanReturnValue(builder, stream);

    }

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
        var timeSpan = TimeSpan.newBuilder();
        long firstSecond = Long.MAX_VALUE;
        long lastSecond = Long.MIN_VALUE;
        var isEmpty = true;

        for (var key : keys) {
            var temporalStatements = value.getMap().get(key);
            if (temporalStatements != null) {
                var statements = temporalStatements.getStatements();
                if (statements != null && statements.size() > 0) {
                    for (var s : statements) {

                        var tp = extractTimePrimitive(s);
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
            }
        }

        if (isEmpty) return null;

        return TimeSpanValue.newBuilder()
                .setFirstSecond(firstSecond)
                .setLastSecond(lastSecond)
                .setTimeSpan(timeSpan.build())
                .build();
    }

    public static TimePrimitive extractTimePrimitive(ProjectStatementValue value) {
        var a = value.getStatement();
        if (a == null) return null;

        var b = a.getObjectLiteral();
        if (b == null) return null;

        return b.getTimePrimitive();
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
            case "71_out":
                timeSpanBuilder.setP81(ntp);
            case "72_out":
                timeSpanBuilder.setP82(ntp);
            case "150_out":
                timeSpanBuilder.setP81a(ntp);
            case "151_out":
                timeSpanBuilder.setP81b(ntp);
            case "152_out":
                timeSpanBuilder.setP82a(ntp);
            case "153_out":
                timeSpanBuilder.setP82b(ntp);
        }
    }

    public static long createFirstSecond(TimePrimitive value) {
        return (long) value.getJulianDay() * ProjectEntityTimeSpan.secondsPerDay;
    }

    public static long createLastSecond(TimePrimitive value) {

        var date = LocalDate.MIN.with(JulianFields.JULIAN_DAY, value.getJulianDay());

        /*
        var calendar = new GregorianCalendar();
        // from docs:
        // To obtain a pure Julian calendar, set the change date to Date(Long.MAX_VALUE).
        calendar.setGregorianChange(new Date(Long.MAX_VALUE));
        GregorianCalendar gc = GregorianCalendar.from(date.atStartOfDay(ZoneId.of("Europe/Paris")));
        gc.isLeapYear();
        */
        switch (value.getDuration()) {
            case "1 year":
                date = date.plus(1, ChronoUnit.YEARS);
            case "1 month":
                date = date.plus(1, ChronoUnit.MONTHS);
            case "1 day":
                date = date.plus(1, ChronoUnit.DAYS);
        }

        long newJulianDay = date.getLong(JulianFields.JULIAN_DAY);

        return newJulianDay * ProjectEntityTimeSpan.secondsPerDay - 1;
    }


    public enum input {
        TOPICS;
        public final String project_entity_top_statements = ProjectEntityTopStatements.output.TOPICS.project_entity_top_statements;


    }


    public enum output {
        TOPICS;
        public final String project_entity_time_span = Utils.tsPrefixed("project_entity_time_span");

    }

}
