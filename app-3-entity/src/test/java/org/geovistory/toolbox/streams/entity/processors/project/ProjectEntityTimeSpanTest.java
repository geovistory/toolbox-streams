package org.geovistory.toolbox.streams.entity.processors.project;


import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.kafka.streams.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.InputTopicNames;
import org.geovistory.toolbox.streams.entity.OutputTopicNames;
import org.geovistory.toolbox.streams.testlib.FileRemover;
import org.geovistory.toolbox.streams.testlib.TopologyTestDriverProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
class ProjectEntityTimeSpanTest {

    @Inject
    Topology topology;

    @Inject
    ConfiguredAvroSerde as;

    @Inject
    OutputTopicNames outputTopicNames;
    @Inject
    InputTopicNames inputTopicNames;
    @ConfigProperty(name = "kafka-streams.state.dir")
    public String stateDir;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopOutgoingStatementsTopic;

    private TestOutputTopic<ProjectEntityKey, TimeSpanValue> outputTopic;


    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");

        testDriver = new TopologyTestDriver(topology, props);

        projectTopOutgoingStatementsTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectTopOutgoingStatements(),
                as.kS(),
                as.vS());


        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectEntityTimeSpan(),
                as.kD(),
                as.vD());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
        FileRemover.removeDir(this.stateDir);
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
