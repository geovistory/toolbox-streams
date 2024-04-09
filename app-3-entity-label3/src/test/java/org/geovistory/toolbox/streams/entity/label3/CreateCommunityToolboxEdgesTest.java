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

import java.util.ArrayList;

import static org.geovistory.toolbox.streams.entity.label3.names.Constants.DEFAULT_PROJECT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
public class CreateCommunityToolboxEdgesTest {

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
    TestInputTopic<ProjectClassKey, EntityLabelConfigTmstp> labelConfigByProjectClassInputTopic;
    TestInputTopic<String, EdgeValue> projectEdgesInputTopic;
    TestOutputTopic<String, LabelEdge> labelEdgesBySourceOutputTopic;
    TestOutputTopic<String, LabelEdge> labelEdgesToolboxCommunityTopic;


    @BeforeEach
    public void setUp() {
        testDriver = new TopologyTestDriver(topology);
        labelConfigByProjectClassInputTopic = testDriver.createInputTopic(
                outputTopicNames.labelConfigByProjectClass(),
                as.kS(), as.vS()
        );
        projectEdgesInputTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectEdges(),
                Serdes.String().serializer(), as.<EdgeValue>value().serializer()
        );
        labelEdgesBySourceOutputTopic = testDriver.createOutputTopic(
                outputTopicNames.labelEdgeBySource(),
                Serdes.String().deserializer(), as.<LabelEdge>value().deserializer()
        );
        labelEdgesToolboxCommunityTopic = testDriver.createOutputTopic(
                outputTopicNames.labelEdgesToolboxCommunityBySource(),
                Serdes.String().deserializer(), as.<LabelEdge>value().deserializer()
        );

    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
        FileRemover.removeDir(this.stateDir);

    }

    @Test
    public void testAddEdgesVisibility() {

        // TODO test that edges with hidden target entity are not passing through!!

        // Publish test input
        sendConfig(DEFAULT_PROJECT.get(), 21, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(123, true, 1)
                });
        sendConfig(DEFAULT_PROJECT.get(), 22, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(234, true, 1)
                });
        sendEdgeWithEntity(true, true, 1, 1, 1, "i1", 21, 123, true, "i2", false);
        sendEdgeWithLiteral(true, 2, 3, 1, "i2", 22, 234, true, "i3", false);

        sendEdgeWithLiteral(true, 2, 7, 2, "i2", 22, 234, true, "i3", false);
        sendEdgeWithLiteral(true, 2, 8, 3, "i2", 22, 234, true, "i3", false);

        var resBySource = labelEdgesBySourceOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(4, resBySource.size());
        assertEquals("Foo", resBySource.get("1_i2_234_o_i3").getTargetLabel());
        assertEquals("Foo", resBySource.get("i2_1_1_i1_123_o_i2").getTargetLabel());

        var edgesToolboxCommunity = labelEdgesToolboxCommunityTopic.readKeyValuesToMap();

        // test toolbox community label edges
        assertEquals(2, edgesToolboxCommunity.size());
        assertEquals(6F, edgesToolboxCommunity.get("i2_234_true_i3").getOrdNum());
    }


    @Test
    public void testRemoveEdgesVisibility() {
        // Publish test input
        sendConfig(DEFAULT_PROJECT.get(), 21, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(123, true, 1)
                });
        sendConfig(DEFAULT_PROJECT.get(), 22, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(234, true, 1)
                });
        sendEdgeWithEntity(true, true, 1, 1, 1, "i1", 21, 123, true, "i2", false);
        sendEdgeWithLiteral(true, 2, 3, 1, "i2", 22, 234, true, "i3", false);

        sendEdgeWithLiteral(true, 2, 7, 2, "i2", 22, 234, true, "i3", false);
        sendEdgeWithLiteral(true, 2, 8, 3, "i2", 22, 234, true, "i3", false);

        var edgesToolboxCommunity = labelEdgesToolboxCommunityTopic.readKeyValuesToMap();

        // test toolbox community label edges
        assertEquals(2, edgesToolboxCommunity.size());
        assertEquals(6F, edgesToolboxCommunity.get("i2_234_true_i3").getOrdNum());

        // delete one project edge
        sendEdgeWithLiteral(true, 2, 8999, 3, "i2", 22, 234, true, "i3", true);
        edgesToolboxCommunity = labelEdgesToolboxCommunityTopic.readKeyValuesToMap();
        assertEquals(5F, edgesToolboxCommunity.get("i2_234_true_i3").getOrdNum());

        // delete all project edges
        sendEdgeWithLiteral(true, 2, 3, 1, "i2", 22, 234, true, "i3", true);
        sendEdgeWithLiteral(true, 2, 7, 2, "i2", 22, 234, true, "i3", true);
        edgesToolboxCommunity = labelEdgesToolboxCommunityTopic.readKeyValuesToMap();
        // should delete this edge
        assertEquals(true, edgesToolboxCommunity.get("i2_234_true_i3").getDeleted());
        // and also related edges, where entity label is removed
        assertEquals(true, edgesToolboxCommunity.get("i1_123_true_i2").getDeleted());


    }


    @Test
    // test that edges with hidden target entity are not published to toolbox community
    public void testEdgeWithHiddenTargetEntity() {


        // Publish test input
        sendConfig(DEFAULT_PROJECT.get(), 21, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(123, true, 1)
                });
        sendConfig(DEFAULT_PROJECT.get(), 22, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(234, true, 1)
                });

        // add edge that should be visible to toolbox community
        sendEdgeWithLiteral(true, 2, 3, 1, "i2", 22, 234, true, "i3", false);

        // add edge with hidden target entity (should not be visible to toolbox community)
        sendEdgeWithEntity(true, false, 1, 1, 1, "i1", 21, 123, true, "i2", false);

        // assure only the label edge with literal is added
        var edgesToolboxCommunity = labelEdgesToolboxCommunityTopic.readKeyValuesToMap();
        assertEquals(1, edgesToolboxCommunity.size());
        assertNotNull(edgesToolboxCommunity.get("i2_234_true_i3"));

        // make the hidden edge visible
        sendEdgeWithEntity(true, true, 1, 1, 1, "i1", 21, 123, true, "i2", false);

        // assure the edge is added
        edgesToolboxCommunity = labelEdgesToolboxCommunityTopic.readKeyValuesToMap();
        assertEquals(1, edgesToolboxCommunity.size());
        assertNotNull(edgesToolboxCommunity.get("i1_123_true_i2"));
    }


    public void sendEdgeWithEntity(boolean sourceCommunityToolbox, boolean targetCommunityToolbox, int statementId, float ordNum, int pid, String sourceId, int sourceClassId, int propertyId, boolean isOutgoing, String targetId, boolean deleted) {
        var v = createEdgeWithEntity(sourceCommunityToolbox, targetCommunityToolbox, statementId, ordNum, pid, sourceId, sourceClassId, propertyId, isOutgoing, targetId, deleted);
        var k = createEdgeKey(v);
        this.projectEdgesInputTopic.pipeInput(k, v);
    }

    public void sendEdgeWithLiteral(boolean sourceCommunityToolbox, int statementId, float ordNum, int pid, String sourceId, int sourceClassId, int propertyId, boolean isOutgoing, String targetId, boolean deleted) {
        var v = createEdgeWithLiteral(sourceCommunityToolbox, statementId, ordNum, pid, sourceId, sourceClassId, propertyId, isOutgoing, targetId, deleted);
        var k = createEdgeKey(v);
        this.projectEdgesInputTopic.pipeInput(k, v);
    }

    public static String createEdgeKey(EdgeValue e) {
        return createEdgeKey(e.getProjectId(), e.getSourceId(), e.getPropertyId(), e.getIsOutgoing(), e.getTargetId());
    }

    public static String createEdgeKey(int projectId, String sourceId, int propertyId, boolean isOutgoing, String targetId) {
        return projectId + "_" + sourceId + "_" + propertyId + "_" + (isOutgoing ? "o" : "i") + "_" + targetId;
    }

    private static EdgeValue createEdgeWithEntity(boolean sourceCommunityToolbox,
                                                  boolean targetCommunityToolbox,
                                                  int statementId, float ordNum, int pid, String sourceId, int sourceClassId, int propertyId, boolean isOutgoing, String targetId, boolean deleted) {
        var e = createEdge(sourceCommunityToolbox, statementId, ordNum, pid, sourceId, sourceClassId, propertyId, isOutgoing, targetId, deleted);
        e.getTargetNode().setEntity(
                Entity.newBuilder()
                        .setFkClass(0)
                        .setCommunityVisibilityWebsite(true)
                        .setCommunityVisibilityDataApi(true)
                        .setCommunityVisibilityToolbox(targetCommunityToolbox)
                        .build());
        return e;
    }

    private static EdgeValue createEdgeWithLiteral(boolean sourceCommunityToolbox, int statementId, float ordNum, int pid, String sourceId, int sourceClassId, int propertyId, boolean isOutgoing, String targetId, boolean deleted) {
        var e = createEdge(sourceCommunityToolbox, statementId, ordNum, pid, sourceId, sourceClassId, propertyId, isOutgoing, targetId, deleted);
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

    private static EdgeValue createEdge(boolean sourceCommunityToolbox,
                                        int statementId, float ordNum, int pid, String sourceId, int sourceClassId, int propertyId, boolean isOutgoing, String targetId, boolean deleted) {
        return EdgeValue.newBuilder()
                .setProjectId(pid)
                .setStatementId(statementId)
                .setProjectCount(0)
                .setOrdNum(ordNum)
                .setSourceId(sourceId)
                .setSourceEntity(Entity.newBuilder()
                        .setFkClass(sourceClassId)
                        .setCommunityVisibilityWebsite(false)
                        .setCommunityVisibilityDataApi(false)
                        .setCommunityVisibilityToolbox(sourceCommunityToolbox)
                        .build())
                .setSourceProjectEntity(EntityValue.newBuilder()
                        .setProjectId(0)
                        .setEntityId("0")
                        .setClassId(sourceClassId)
                        .setCommunityVisibilityToolbox(sourceCommunityToolbox)
                        .setCommunityVisibilityDataApi(true)
                        .setCommunityVisibilityWebsite(true)
                        .setProjectVisibilityDataApi(true)
                        .setProjectVisibilityWebsite(true)
                        .setDeleted(false)
                        .build())
                .setPropertyId(propertyId)
                .setIsOutgoing(isOutgoing)
                .setTargetId(targetId)
                .setTargetNode(NodeValue.newBuilder().setLabel("").setId(targetId).setClassId(0).build())
                .setTargetProjectEntity(EntityValue.newBuilder()
                        .setProjectId(0)
                        .setEntityId(targetId)
                        .setClassId(0)
                        .setCommunityVisibilityToolbox(true)
                        .setCommunityVisibilityDataApi(true)
                        .setCommunityVisibilityWebsite(true)
                        .setProjectVisibilityDataApi(true)
                        .setProjectVisibilityWebsite(true)
                        .setDeleted(false)
                        .build())
                .setModifiedAt("0")
                .setDeleted(deleted)
                .build();
    }

    public ProjectClassKey sendConfig(int projectId, int classId, Long timestamp, EntityLabelConfigPartField[] parts) {
        return sendConfig(projectId, classId, timestamp, parts, false);
    }

    public ProjectClassKey sendConfig(int projectId, int classId, Long timestamp, EntityLabelConfigPartField[] parts, Boolean deleted) {

        var labelparts = new ArrayList<EntityLabelConfigPart>();
        var i = 1;
        for (var item : parts) {
            labelparts.add(EntityLabelConfigPart.newBuilder().setOrdNum(i).setField(item).build());
            i++;
        }

        var k = ProjectClassKey.newBuilder().setProjectId(projectId).setClassId(classId).build();
        var v = EntityLabelConfigTmstp.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setConfig(
                        EntityLabelConfig.newBuilder().setLabelParts(labelparts).build()
                )
                .setRecordTimestamp(timestamp)
                .setDeleted(deleted)
                .build();

        labelConfigByProjectClassInputTopic.pipeInput(k, v);
        return k;
    }
}
