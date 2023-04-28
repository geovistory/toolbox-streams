package org.geovistory.toolbox.streams.entity.processors.project;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectEntityTimeSpanTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectEntityTimeSpanTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopOutgoingStatementsTopic;

    private TestOutputTopic<ProjectEntityKey, TimeSpanValue> outputTopic;


    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");

        var builderSingleton = new BuilderSingleton();
        var avroSerdes = new AvroSerdes();
        avroSerdes.QUARKUS_KAFKA_STREAMS_SCHEMA_REGISTRY_URL = MOCK_SCHEMA_REGISTRY_URL;
        var inputTopicNames = new InputTopicNames();
        var outputTopicNames = new OutputTopicNames();
        var registerInputTopic = new RegisterInputTopic(avroSerdes, builderSingleton, inputTopicNames);
        var communityClassLabel = new ProjectEntityTimeSpan(avroSerdes, registerInputTopic, outputTopicNames);
        communityClassLabel.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        projectTopOutgoingStatementsTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectTopOutgoingStatements(),
                avroSerdes.ProjectTopStatementsKey().serializer(),
                avroSerdes.ProjectTopStatementsValue().serializer());


        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectEntityTimeSpan(),
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


        int ongoingThroughout = 71;
        var k = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .setPropertyId(ongoingThroughout)
                .setIsOutgoing(true)
                .build();
        var v = ProjectTopStatementsValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(ongoingThroughout)
                .setEntityId(entityId).setIsOutgoing(true).setEdges(List.of(
                        ProjectEdgeValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNum(1)
                                .setSourceId(entityId)
                                .setTargetId(entityId)
                                .setPropertyId(ongoingThroughout)
                                .setTargetNode(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setTimePrimitive(TimePrimitive.newBuilder()
                                                .setJulianDay(2362729)
                                                .setDuration("1 day")
                                                .setCalendar("gregorian")
                                                .build())
                                        .build()
                                ).build()
                )).build();


        projectTopOutgoingStatementsTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var entityKey = ProjectEntityKey.newBuilder().setProjectId(projectId).setEntityId(entityId).build();
        var record = outRecords.get(entityKey);

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

        var k = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .setPropertyId(nonTemporalProperty)
                .setIsOutgoing(true)
                .build();

        var v = ProjectTopStatementsValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(nonTemporalProperty)
                .setEntityId(entityId).setIsOutgoing(true).setEdges(List.of(
                        ProjectEdgeValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNum(1)
                                .setSourceId(entityId)
                                .setTargetId(entityId)
                                .setPropertyId(nonTemporalProperty)
                                .setTargetNode(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setTimePrimitive(TimePrimitive.newBuilder()
                                                .setJulianDay(2362729)
                                                .setDuration("1 day")
                                                .setCalendar("gregorian")
                                                .build())
                                        .build()
                                ).build()
                )).build();


        projectTopOutgoingStatementsTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isTrue();
    }


}
