package org.geovistory.toolbox.streams.analysis.statements.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.analysis.statements.AnalysisConfluentAvroSerdes;
import org.geovistory.toolbox.streams.analysis.statements.RegisterInputTopic;
import org.geovistory.toolbox.streams.analysis.statements.avro.*;
import org.geovistory.toolbox.streams.avro.AnalysisStatementKey;
import org.geovistory.toolbox.streams.avro.NodeValue;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementValue;
import org.geovistory.toolbox.streams.lib.GeoUtils;
import org.geovistory.toolbox.streams.lib.JsonStringifier;
import org.geovistory.toolbox.streams.lib.TimeUtils;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.List;
import java.util.Objects;


public class ProjectAnalysisStatement {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var inputTopic = new RegisterInputTopic(builder);

        return addProcessors(
                builder,
                inputTopic.projectStatementWithLiteral(),
                inputTopic.projectStatementWithEntity()
        ).builder().build();
    }

    public static ProjectAnalysisStatementReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithLiteral,
            KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntity
    ) {

        var avroSerdes = new AnalysisConfluentAvroSerdes();
        ObjectMapper mapper = JsonStringifier.getMapperIgnoringNulls();

        /* STREAM PROCESSORS */
        // 2)
        var merged = projectStatementWithLiteral.merge(projectStatementWithEntity);


        var mapped = merged.map((key, value) -> {
            var object = mapObject(value.getStatement().getObject());
            try {
                var objectJsonString = mapper.writeValueAsString(object);

                var k = AnalysisStatementKey.newBuilder()
                        .setPkEntity(key.getStatementId())
                        .setProject(key.getProjectId())
                        .build();
                var v = AnalysisStatementValue.newBuilder()
                        .setPkEntity(key.getStatementId())
                        .setProject(key.getProjectId())
                        .setFkProject(key.getProjectId())
                        .setFkSubjectInfo(Utils.parseStringId(value.getStatement().getSubjectId()))
                        .setFkProperty(value.getStatement().getPropertyId())
                        .setFkObjectInfo(Utils.parseStringId(value.getStatement().getObjectId()))
                        .setIsInProjectCount(1)
                        .setOrdNumOfDomain(value.getOrdNumOfDomain())
                        .setOrdNumOfRange(value.getOrdNumOfRange())
                        .setObjectInfoValue(objectJsonString)
                        .build();

                return KeyValue.pair(k, v);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
        /* SINK PROCESSORS */


        mapped.to(output.TOPICS.project_analysis_statement,
                Produced.with(avroSerdes.AnalysisStatementKey(), avroSerdes.AnalysisStatementValue())
                        .withName(output.TOPICS.project_analysis_statement + "-producer")
        );

        return new ProjectAnalysisStatementReturnValue(builder, merged);

    }


    private static ObjectInfoValue mapObject(NodeValue object) {
        var v = ObjectInfoValue.newBuilder();

        if (object.getAppellation() != null) {

            // Appellation

            var a = object.getAppellation();
            v.setString(AnalysisString.newBuilder()
                    .setPkEntity(a.getPkEntity())
                    .setFkClass(a.getFkClass())
                    .setString(a.getString())
                    .build()).build();

        } else if (object.getPlace() != null) {

            // Place

            var o = object.getPlace();
            var wkb = o.getGeoPoint().getWkb();
            var point = GeoUtils.bytesToPoint(wkb);
            var coordinates = List.of(point.getX(), point.getY());
            v.setGeometry(AnalysisGeometry.newBuilder()
                    .setPkEntity(o.getPkEntity())
                    .setFkClass(o.getFkClass())
                    .setGeoJSON(GeoJson.newBuilder()
                            .setCoordinates(coordinates)
                            .setType("Point")
                            .build()).build());
        } else if (object.getLanguage() != null) {

            // Language

            var o = object.getLanguage();
            v.setLanguage(AnalysisLanguage.newBuilder()
                    .setPkEntity(o.getPkEntity())
                    .setFkClass(o.getFkClass())
                    .setLabel(o.getNotes())
                    .setIso6391(o.getSetIso6391())
                    .setIso6392b(o.getSetIso6392b())
                    .setIso6392t(o.getSetIso6392t())
                    .build()
            ).build();
        } else if (object.getTimePrimitive() != null) {

            // TimePrimitive

            var o = object.getTimePrimitive();

            var fromDay = o.getJulianDay();
            var from = getBoundary(fromDay);

            var toDay = TimeUtils.getJulianDayPlusDuration(fromDay, o.getDuration());
            var to = getBoundary(toDay);

            var labelPart1 = Objects.equals(o.getCalendar(), "gregorian") ? from.getCalGregorian() : from.getCalJulian();
            var label = labelPart1 + " (" + o.getDuration() + ")";

            v.setTimePrimitive(AnalysisTimePrimitive.newBuilder()
                    .setPkEntity(o.getPkEntity())
                    .setFkClass(o.getFkClass())
                    .setJulianDay(o.getJulianDay())
                    .setCalendar(o.getCalendar())
                    .setDuration(o.getDuration())
                    .setFrom(from)
                    .setTo(to)
                    .setLabel(label)
                    .build()
            ).build();
        } else if (object.getLangString() != null) {

            // LangString

            var o = object.getLangString();
            v.setLangString(AnalysisLangString.newBuilder()
                    .setPkEntity(o.getPkEntity())
                    .setFkClass(o.getFkClass())
                    .setString(o.getString())
                    .setFkLanguage(o.getFkLanguage())
                    .build()
            ).build();
        } else if (object.getDimension() != null) {

            // Dimension

            var o = object.getDimension();
            v.setDimension(AnalysisDimension.newBuilder()
                    .setPkEntity(o.getPkEntity())
                    .setFkClass(o.getFkClass())
                    .setNumericValue(o.getNumericValue())
                    .setFkMeasurementUnit(o.getFkMeasurementUnit())
                    .build()
            ).build();
        } else if (object.getCell() != null) {

            // Cell

            var o = object.getCell();
            v.setCell(AnalysisCell.newBuilder()
                    .setPkCell(o.getPkCell())
                    .setFkClass(o.getFkClass())
                    .setFkColumn(o.getFkColumn())
                    .setFkRow(o.getFkRow())
                    .setNumericValue(o.getNumericValue())
                    .setStringValue(o.getStringValue())
                    .build()
            ).build();
        }

        return v.build();

    }

    private static AnalysisTimePrimitiveBoundary getBoundary(Integer julianDay) {
        var julianSecond = TimeUtils.getJulianSecond(julianDay);
        var julianYMD = TimeUtils.getYearMonthDay(julianDay, TimeUtils.CalendarType.julian);
        var gregorianYMD = TimeUtils.getYearMonthDay(julianDay, TimeUtils.CalendarType.gregorian);
        var fromIso8601 = TimeUtils.getIso8601String(julianDay);


        return AnalysisTimePrimitiveBoundary.newBuilder()
                .setJulianDay(julianDay)
                .setJulianSecond(julianSecond)
                .setCalJulian(julianYMD.toString())
                .setCalGregorian(gregorianYMD.toString())
                .setCalGregorianIso8601(fromIso8601)
                .build();
    }


    public enum output {
        TOPICS;
        public final String project_analysis_statement = Utils.tsPrefixed("project_analysis_statement");
    }



}
