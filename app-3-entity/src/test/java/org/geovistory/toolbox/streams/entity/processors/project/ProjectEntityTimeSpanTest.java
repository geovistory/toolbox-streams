package org.geovistory.toolbox.streams.entity.processors.project;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectEntityTimeSpanTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectEntityTimeSpanTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectEntityKey, ProjectEntityTopStatementsValue> projectEntityTopStatementsTopic;

    private TestOutputTopic<ProjectEntityKey, TimeSpanValue> outputTopic;


    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = ProjectEntityTimeSpan.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        projectEntityTopStatementsTopic = testDriver.createInputTopic(
                ProjectEntityTimeSpan.input.TOPICS.project_entity_top_statements,
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityTopStatementsValue().serializer());


        outputTopic = testDriver.createOutputTopic(
                ProjectEntityTimeSpan.output.TOPICS.project_entity_time_span,
                avroSerdes.ProjectEntityKey().deserializer(),
                avroSerdes.TimeSpanValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testTopology() {
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


        var k = ProjectEntityKey.newBuilder().setProjectId(projectId).setEntityId(entityId).build();
        projectEntityTopStatementsTopic.pipeInput(k, entityTopStatements);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var record = outRecords.get(k);

        assertThat(record.getTimeSpan().getP81().getDuration()).isEqualTo("1 day");
        assertThat(record.getFirstSecond()).isEqualTo(expectedFirstSec);
        assertThat(record.getLastSecond()).isEqualTo(expectedLastSec);
    }

    @Test
    void testTopologyWithoutTemproalData() {
        var projectId = 1;
        var classId = 2;
        var entityId = "foo";
        var nonTemporalProperty = 1;

        var map = new HashMap<String, ProjectTopStatementsWithPropLabelValue>();

        var nonTemporalStatements = ProjectTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(nonTemporalProperty)
                .setEntityId(entityId).setIsOutgoing(true).setPropertyLabel("nonTemporalProperty").setStatements(List.of(
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(1)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(nonTemporalProperty)
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


        map.put(nonTemporalProperty + "_out", nonTemporalStatements);

        var entityTopStatements = ProjectEntityTopStatementsValue.newBuilder()
                .setEntityId(entityId).setProjectId(projectId).setClassId(classId).setMap(map).build();


        var k = ProjectEntityKey.newBuilder().setProjectId(projectId).setEntityId(entityId).build();
        projectEntityTopStatementsTopic.pipeInput(k, entityTopStatements);

        assertThat(outputTopic.isEmpty()).isTrue();
    }


}