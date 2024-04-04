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


@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
public class CreateEntityLangLabelForRdfTest {

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
    TestInputTopic<String, LabelEdge> labelEdgeBySourceInputTopic;
    TestInputTopic<String, LabelEdge> labelEdgeByTargetInputTopic;
    TestOutputTopic<ProjectLabelGroupKey, ProjectRdfValue> tCLOutput;
    TestOutputTopic<ProjectLabelGroupKey, ProjectRdfValue> pCLOutput;


    @BeforeEach
    public void setUp() {
        testDriver = new TopologyTestDriver(topology);
        labelConfigByProjectClassInputTopic = testDriver.createInputTopic(
                outputTopicNames.labelConfigByProjectClass(),
                as.kS(), as.vS()
        );

        labelEdgeBySourceInputTopic = testDriver.createInputTopic(
                outputTopicNames.labelEdgeBySource(),
                Serdes.String().serializer(), as.vS()
        );

        labelEdgeByTargetInputTopic = testDriver.createInputTopic(
                outputTopicNames.labelEdgeByTarget(),
                Serdes.String().serializer(), as.vS()
        );

        tCLOutput = testDriver.createOutputTopic(
                outputTopicNames.entityLanguageLabelsToolboxCommunity(),
                as.kD(), as.vD()
        );
        pCLOutput = testDriver.createOutputTopic(
                outputTopicNames.entityLanguageLabelsPublicCommunity(),
                as.kD(), as.vD()
        );

    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
        FileRemover.removeDir(this.stateDir);

    }


    // Test entity language labels are created for community entities
    @Test
    public void testCommunityLanguageLabels() {
        // Publish test input
        sendConfig(DEFAULT_PROJECT.get(), 3, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(4, true, 1)
                });
        sendLabelEdge(1, 3, "i2", 4, true, 1f, "", "i3", "Foo", "en", true, false);
        sendLabelEdge(4, 3, "i2", 4, true, 1f, "", "i3", "Bar 2", "fr", true, false);
        sendLabelEdge(2, 3, "i2", 4, true, 1f, "", "i3", "Bar", "fr", true, false);
        sendLabelEdge(3, 3, "i2", 4, true, 1f, "", "i3", "Baz", "de", true, false);
        sendLabelEdge(4, 3, "i2", 4, true, 1f, "", "i3", "Bar", "fr", true, false);


        var entityLabels = tCLOutput.readKeyValuesToList();

        assertEquals(Operation.insert, entityLabels.get(0).value.getOperation());
        assertEquals("Foo", entityLabels.get(0).key.getLabel());
        assertEquals("en", entityLabels.get(0).key.getLanguage());
        assertEquals(0, entityLabels.get(0).key.getProjectId());

        assertEquals(Operation.insert, entityLabels.get(1).value.getOperation());
        assertEquals("Bar 2", entityLabels.get(1).key.getLabel());
        assertEquals("fr", entityLabels.get(1).key.getLanguage());
        assertEquals(0, entityLabels.get(1).key.getProjectId());

        assertEquals(Operation.delete, entityLabels.get(2).value.getOperation());
        assertEquals("Bar 2", entityLabels.get(2).key.getLabel());
        assertEquals("fr", entityLabels.get(2).key.getLanguage());
        assertEquals(0, entityLabels.get(2).key.getProjectId());

        assertEquals(Operation.insert, entityLabels.get(3).value.getOperation());
        assertEquals("Bar", entityLabels.get(3).key.getLabel());
        assertEquals("fr", entityLabels.get(3).key.getLanguage());
        assertEquals(0, entityLabels.get(3).key.getProjectId());

        assertEquals(Operation.insert, entityLabels.get(4).value.getOperation());
        assertEquals("Baz", entityLabels.get(4).key.getLabel());
        assertEquals("de", entityLabels.get(4).key.getLanguage());
        assertEquals(0, entityLabels.get(4).key.getProjectId());

    }

    public String sendLabelEdge(Integer project_id, Integer source_class_id, String source_id, Integer property_id, Boolean is_outgoing, Float ord_num, String modified_at, String target_id, String target_label, String target_label_language, Boolean target_is_in_project, Boolean deleted) {
        return sendLabelEdge(
                true,
                true,
                true,
                true,
                project_id,
                source_class_id,
                source_id,
                property_id,
                is_outgoing,
                ord_num,
                modified_at,
                target_id,
                target_label,
                target_label_language,
                target_is_in_project,
                deleted, true);
    }

    public String sendLabelEdge(Integer project_id, Integer source_class_id, String source_id, Integer property_id, Boolean is_outgoing, Float ord_num, String modified_at, String target_id, String target_label, String target_label_language, Boolean target_is_in_project, Boolean deleted, Boolean targetIsLiteral) {
        return sendLabelEdge(
                true,
                true,
                true,
                true,
                project_id,
                source_class_id,
                source_id,
                property_id,
                is_outgoing,
                ord_num,
                modified_at,
                target_id,
                target_label,
                target_label_language,
                target_is_in_project,
                deleted, targetIsLiteral);
    }

    public String sendLabelEdge(boolean source_project_public,
                                boolean source_community_public,
                                boolean source_community_toolbox,
                                boolean edge_community_toolbox, Integer project_id, Integer source_class_id, String source_id, Integer property_id, Boolean is_outgoing, Float ord_num, String modified_at, String target_id, String target_label, String target_label_language, Boolean target_is_in_project, Boolean deleted, Boolean targetIsLiteral) {
        var v = new LabelEdge(
                source_project_public,
                source_community_public,
                source_community_toolbox,
                edge_community_toolbox,
                project_id,
                source_class_id,
                source_id,
                property_id,
                is_outgoing,
                ord_num,
                modified_at,
                target_id,
                target_label,
                target_label_language,
                target_is_in_project,
                deleted);

        var k = String.join("_", new String[]{project_id.toString(), source_id, property_id.toString(), is_outgoing ? "o" : "i", target_id});
        if (targetIsLiteral) this.labelEdgeBySourceInputTopic.pipeInput(k, v);
        else this.labelEdgeByTargetInputTopic.pipeInput(k, v);
        return k;
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
