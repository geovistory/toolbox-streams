package org.geovistory.toolbox.streams.entity.processors.project;


import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serdes;
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

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.geovistory.toolbox.streams.lib.Utils.createEdgeUniqueKey;

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
    private TestInputTopic<String, EdgeValue> edgeInputTopic;

    private TestOutputTopic<ProjectEntityKey, TimeSpanValue> outputTopic;


    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");

        testDriver = new TopologyTestDriver(topology, props);

        edgeInputTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectEdges(),
                Serdes.String().serializer(),
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
        var entityId = "foo";
        long expectedFirstSec = 204139785600L;
        long expectedLastSec = 204139871999L;


        int ongoingThroughout = 71;
        sendEdge(projectId, entityId, ongoingThroughout, "i2", 2362729);

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
    void testTopologyWithoutTemporalData() {
        var projectId = 1;
        var entityId = "foo";
        var nonTemporalProperty = 1;

        sendEdge(projectId, entityId, nonTemporalProperty, "i2", 2362729);
        var outRecords = outputTopic.readKeyValuesToMap();

        var entityKey = ProjectEntityKey.newBuilder().setProjectId(projectId).setEntityId(entityId).build();
        var record = outRecords.get(entityKey);
        assertThat(record).isNull();
    }


    public String sendEdge(int pid, String sourceId, int propertyId, String targetId, int julianDay) {
        var v = createEdgeWithTimePrimitive(pid, sourceId, propertyId, targetId, julianDay);
        var k = createEdgeUniqueKey(v);
        this.edgeInputTopic.pipeInput(k, v);
        return k;
    }


    private static EdgeValue createEdgeWithTimePrimitive(int pid, String sourceId, int propertyId, String targetId, int julianDay) {
        var e = createEdge(pid, sourceId, propertyId, targetId);
        e.getTargetNode().setLabel("Foo");
        e.getTargetNode().setTimePrimitive(
                TimePrimitive.newBuilder()
                        .setJulianDay(julianDay)
                        .setDuration("1 day")
                        .setCalendar("gregorian")
                        .build()
        );
        return e;
    }

    private static EdgeValue createEdge(int pid, String sourceId, int propertyId, String targetId) {
        return EdgeValue.newBuilder()
                .setProjectId(pid)
                .setStatementId(0)
                .setProjectCount(0)
                .setOrdNum(0f)
                .setSourceId(sourceId)
                .setSourceEntity(Entity.newBuilder()
                        .setFkClass(0)
                        .setCommunityVisibilityWebsite(false)
                        .setCommunityVisibilityDataApi(false)
                        .setCommunityVisibilityToolbox(false)
                        .build())
                .setSourceProjectEntity(EntityValue.newBuilder()
                        .setProjectId(0)
                        .setEntityId("0")
                        .setClassId(0)
                        .setCommunityVisibilityToolbox(true)
                        .setCommunityVisibilityDataApi(true)
                        .setCommunityVisibilityWebsite(true)
                        .setProjectVisibilityDataApi(true)
                        .setProjectVisibilityWebsite(true)
                        .setDeleted(false)
                        .build())
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setTargetId(targetId)
                .setTargetNode(NodeValue.newBuilder().setLabel("").setId(targetId).setClassId(0).build())
                .setTargetProjectEntity(null)
                .setModifiedAt("0")
                .setDeleted(false)
                .build();
    }

}
