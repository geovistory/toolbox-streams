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
import static org.junit.jupiter.api.Assertions.assertNotNull;


@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
public class CreateEntityLabelForRdfTest {

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
    TestOutputTopic<ProjectLabelGroupKey, ProjectRdfValue> tPOutput;
    TestOutputTopic<ProjectLabelGroupKey, ProjectRdfValue> tCOutput;
    TestOutputTopic<ProjectLabelGroupKey, ProjectRdfValue> pPOutput;
    TestOutputTopic<ProjectLabelGroupKey, ProjectRdfValue> pCOutput;


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
        tPOutput = testDriver.createOutputTopic(
                outputTopicNames.entityLabelsToolboxProject(),
                as.kD(), as.vD()
        );
        tCOutput = testDriver.createOutputTopic(
                outputTopicNames.entityLabelsToolboxCommunity(),
                as.kD(), as.vD()
        );
        pPOutput = testDriver.createOutputTopic(
                outputTopicNames.entityLabelsPublicProject(),
                as.kD(), as.vD()
        );
        pCOutput = testDriver.createOutputTopic(
                outputTopicNames.entityLabelsPublicCommunity(),
                as.kD(), as.vD()
        );

    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
        FileRemover.removeDir(this.stateDir);

    }

    @Test
    public void testEntityLabelOutputForRdf() {
        // Publish test input

        sendConfig(DEFAULT_PROJECT.get(), 22, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(234, true, 1)
                });
        sendEdgeWithLiteral(true, true, true, 1, 3, 1, "i2", 22, 234, true, "i3", false);


        var k = ProjectLabelGroupKey.newBuilder().setLabel("Foo").setEntityId("i2").setLanguage("aa").setProjectId(1).build();
        var tpRecordMap = tPOutput.readKeyValuesToMap();

        // test label edge by source
        assertNotNull(tpRecordMap.get(k));
    }


    public void sendEdgeWithLiteral(
            boolean toolboxCommunity,
            boolean publicCommunity,
            boolean publicProject, int statementId, float ordNum, int pid, String sourceId, int sourceClassId, int propertyId, boolean isOutgoing, String targetId, boolean deleted) {
        var v = createEdgeWithLiteral(toolboxCommunity, publicCommunity, publicProject, statementId, ordNum, pid, sourceId, sourceClassId, propertyId, isOutgoing, targetId, deleted);
        var k = createEdgeKey(v);
        this.projectEdgesInputTopic.pipeInput(k, v);
    }

    public static String createEdgeKey(EdgeValue e) {
        return createEdgeKey(e.getProjectId(), e.getSourceId(), e.getPropertyId(), e.getIsOutgoing(), e.getTargetId());
    }

    public static String createEdgeKey(int projectId, String sourceId, int propertyId, boolean isOutgoing, String targetId) {
        return projectId + "_" + sourceId + "_" + propertyId + "_" + (isOutgoing ? "o" : "i") + "_" + targetId;
    }

    private static EdgeValue createEdgeWithLiteral(
            boolean toolboxCommunity,
            boolean publicCommunity,
            boolean publicProject, int statementId, float ordNum, int pid, String sourceId, int sourceClassId, int propertyId, boolean isOutgoing, String targetId, boolean deleted) {
        var e = createEdge(toolboxCommunity, publicCommunity, publicProject, statementId, ordNum, pid, sourceId, sourceClassId, propertyId, isOutgoing, targetId, deleted);
        e.getTargetNode().setLabel("Foo");
        e.getTargetNode().setLangString(
                LangString.newBuilder()
                        .setPkEntity(0)
                        .setFkClass(0)
                        .setString("Foo")
                        .setFkLanguage(17082)
                        .build()
        );
        return e;
    }

    private static EdgeValue createEdge(
            boolean toolboxCommunity,
            boolean publicCommunity,
            boolean publicProject,
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
                        .setCommunityVisibilityDataApi(publicCommunity)
                        .setCommunityVisibilityToolbox(toolboxCommunity)
                        .build())
                .setSourceProjectEntity(EntityValue.newBuilder()
                        .setProjectId(0)
                        .setEntityId("0")
                        .setClassId(sourceClassId)
                        .setCommunityVisibilityToolbox(toolboxCommunity)
                        .setCommunityVisibilityDataApi(publicCommunity)
                        .setCommunityVisibilityWebsite(true)
                        .setProjectVisibilityDataApi(publicProject)
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
