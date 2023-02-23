package org.geovistory.toolbox.streams.entity.processors.lib;


import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.lib.TimeSpanFactory;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TimeSpanFactoryTest {




    @Test
    void testCreateTimeSpanMethod() {
        var projectId = 1;
        var classId = 2;
        var entityId = "foo";
        long expectedFirstSec = 204139785600L;
        long expectedLastSec = 204139871999L;

        var map = new HashMap<String, ProjectTopStatementsWithPropLabelValue>();

        int ongoingThroughout = 71;
        var ongoingThroughoutStatements = ProjectTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(ongoingThroughout)
                .setEntityId(entityId).setIsOutgoing(true).setPropertyLabel("ongoingThroughout").setStatements(List.of(
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(1)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(ongoingThroughout)
                                        .setObject(NodeValue.newBuilder()
                                                .setClassId(0)
                                                .setTimePrimitive(TimePrimitive.newBuilder()
                                                        .setJulianDay(2362729)
                                                        .setDuration("1 day")
                                                        .setCalendar("gregorian")
                                                        .build())
                                                .build()
                                        ).build()
                                ).build()
                )).build();


        map.put(ongoingThroughout + "_out", ongoingThroughoutStatements);

        var entityTopStatements = ProjectEntityTopStatementsValue.newBuilder()
                .setEntityId(entityId).setProjectId(projectId).setClassId(classId).setMap(map).build();

        var result = TimeSpanFactory.createTimeSpan(entityTopStatements);

        assertThat(result).isNotNull();
        assertThat(result.getTimeSpan().getP81().getDuration()).isEqualTo("1 day");
        assertThat(result.getTimeSpan().getP81a()).isNull();
        assertThat(result.getFirstSecond()).isEqualTo(expectedFirstSec);
        assertThat(result.getLastSecond()).isEqualTo(expectedLastSec);
    }

    @Test
    void testMethodWithoutTemporalData() {
        var projectId = 1;
        var classId = 2;
        var entityId = "foo";

        var map = new HashMap<String, ProjectTopStatementsWithPropLabelValue>();


        var entityTopStatements = ProjectEntityTopStatementsValue.newBuilder()
                .setEntityId(entityId).setProjectId(projectId).setClassId(classId).setMap(map).build();

        var result = TimeSpanFactory.createTimeSpan(entityTopStatements);

        assertThat(result).isNull();
    }


}
