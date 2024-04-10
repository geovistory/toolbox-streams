package org.geovistory.toolbox.streams.entity.label3;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label3.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.label3.names.InputTopicNames;
import org.geovistory.toolbox.streams.entity.label3.names.OutputTopicNames;
import org.geovistory.toolbox.streams.testlib.FileRemover;
import org.geovistory.toolbox.streams.testlib.TopologyTestDriverProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
public class CreateLabelEdgesTest {

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
    TopologyTestDriver testDriver;
    TestInputTopic<String, EdgeValue> projectEdgesInputTopic;
    TestOutputTopic<String, LabelEdge> labelEdgesBySourceOutputTopic;
    TestOutputTopic<String, LabelEdge> labelEdgesByTargetOutputTopic;

    @BeforeEach
    public void setUp() {
        testDriver = new TopologyTestDriver(topology);
        projectEdgesInputTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectEdges(),
                Serdes.String().serializer(), as.<EdgeValue>value().serializer()
        );
        labelEdgesBySourceOutputTopic = testDriver.createOutputTopic(
                outputTopicNames.labelEdgeBySource(),
                Serdes.String().deserializer(), as.<LabelEdge>value().deserializer()
        );
        labelEdgesByTargetOutputTopic = testDriver.createOutputTopic(
                outputTopicNames.labelEdgeByTarget(),
                Serdes.String().deserializer(), as.<LabelEdge>value().deserializer()
        );

    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
        FileRemover.removeDir(this.stateDir);

    }

    @Test
    public void createLabelEdgesTest() {
        // Publish test input
        sendEdgeWithEntity(1, "i1");
        sendEdgeWithLiteral(1, "i");

        var resBySource = labelEdgesBySourceOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(1, resBySource.size());
        assertEquals("Foo", resBySource.entrySet().stream().findFirst().get().getValue().getTargetLabel());

        var resByTarget = labelEdgesByTargetOutputTopic.readKeyValuesToMap();

        // test label edge by target
        assertEquals(1, resByTarget.size());
    }

    public void sendEdgeWithEntity(int pid, String sourceId) {
        var v = createEdgeWithEntity(pid, sourceId);
        var k = createEdgeKey(v);
        this.projectEdgesInputTopic.pipeInput(k, v);
    }

    public void sendEdgeWithLiteral(int pid, String sourceId) {
        var v = createEdgeWithLiteral(pid, sourceId);
        var k = createEdgeKey(v);
        this.projectEdgesInputTopic.pipeInput(k, v);
    }

    public static String createEdgeKey(EdgeValue e) {
        return createEdgeKey(e.getProjectId(), e.getSourceId(), e.getPropertyId(), e.getIsOutgoing(), e.getTargetId());
    }

    public static String createEdgeKey(int projectId, String sourceId, int propertyId, boolean isOutgoing, String targetId) {
        return projectId + "_" + sourceId + "_" + propertyId + "_" + (isOutgoing ? "o" : "i") + "_" + targetId;
    }

    private static EdgeValue createEdgeWithEntity(int pid, String sourceId) {
        var e = createEdge(pid, sourceId);
        e.getTargetNode().setEntity(
                Entity.newBuilder()
                        .setFkClass(0)
                        .setCommunityVisibilityWebsite(true)
                        .setCommunityVisibilityDataApi(true)
                        .setCommunityVisibilityToolbox(true)
                        .build());
        return e;
    }

    private static EdgeValue createEdgeWithLiteral(int pid, String sourceId) {
        var e = createEdge(pid, sourceId);
        e.getTargetNode().setLabel("Foo");
        e.getTargetNode().setLangString(
                LangString.newBuilder()
                        .setPkEntity(0)
                        .setFkClass(0)
                        .setString("Foo")
                        .setFkLanguage(123)
                        .build()
        );
        return e;
    }

    private static EdgeValue createEdge(int pid, String sourceId) {
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
                .setPropertyId(0)
                .setIsOutgoing(false)
                .setTargetId("0")
                .setTargetNode(NodeValue.newBuilder().setLabel("").setId("0").setClassId(0).build())
                .setTargetProjectEntity(EntityValue.newBuilder()
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
                .setModifiedAt("0")
                .setDeleted(false)
                .build();
    }
}
