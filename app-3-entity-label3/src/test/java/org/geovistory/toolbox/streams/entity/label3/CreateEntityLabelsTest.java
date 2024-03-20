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
import org.geovistory.toolbox.streams.entity.label3.lib.Fn;
import org.geovistory.toolbox.streams.entity.label3.names.OutputTopicNames;
import org.geovistory.toolbox.streams.testlib.FileRemover;
import org.geovistory.toolbox.streams.testlib.TopologyTestDriverProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;

import static org.geovistory.toolbox.streams.entity.label3.names.Constants.DEFAULT_PROJECT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
public class CreateEntityLabelsTest {

    @Inject
    Topology topology;
    @Inject
    ConfiguredAvroSerde as;
    @Inject
    OutputTopicNames outputTopicNames;
    @ConfigProperty(name = "kafka-streams.state.dir")
    public String stateDir;
    TopologyTestDriver testDriver;
    TestInputTopic<ProjectClassKey, EntityLabelConfigTmstp> labelConfigByProjectClassInputTopic;
    TestInputTopic<String, LabelEdge> labelEdgeBySourceInputTopic;
    TestOutputTopic<ProjectEntityKey, EntityLabel> entityLabelsOutputTopic;

    @BeforeEach
    public void setUp() {
        testDriver = new TopologyTestDriver(topology);
        labelConfigByProjectClassInputTopic = testDriver.createInputTopic(
                outputTopicNames.labelConfigByProjectClass(),
                as.<ProjectClassKey>key().serializer(), as.vS()
        );
        labelEdgeBySourceInputTopic = testDriver.createInputTopic(
                outputTopicNames.labelEdgeBySource(),
                Serdes.String().serializer(), as.vS()
        );
        entityLabelsOutputTopic = testDriver.createOutputTopic(
                outputTopicNames.entityLabels(),
                as.kD(), as.vD()
        );

    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
        FileRemover.removeDir(this.stateDir);

    }

    @Test
    public void createEntityLabel() {
        // Publish test input
        sendConfig(1, 365, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(1113, true, 1)
                });
        sendLabelEdge(1, 365, "i1", 1113, true, 1f, "", "i2", "Foo", "en", true, false);


        var entityLabels = entityLabelsOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(1, entityLabels.size());
        assertEquals("Foo", entityLabels.entrySet().stream().findFirst().get().getValue().getLabel());

    }

    @Test
    public void createEntityLabelOtherWay() {
        // Publish test input
        sendLabelEdge(1, 365, "i1", 1113, true, 1f, "", "i2", "Foo", "en", true, false);
        sendConfig(1, 365, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(1113, true, 1)
                });

        testDriver.advanceWallClockTime(Duration.ofSeconds(10));

        var entityLabels = entityLabelsOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(1, entityLabels.size());
        assertEquals("Foo", entityLabels.entrySet().stream().findFirst().get().getValue().getLabel());

    }

    @Test
    public void createEntityLabelMarkEdgeAsDeleted() {
        // Publish test input
        sendLabelEdge(1, 365, "i1", 1113, true, 1f, "", "i2", "Foo", "en", true, false);
        sendLabelEdge(1, 365, "i1", 1113, true, 1.2f, "", "i3", "Bar", "en", true, false);
        sendConfig(1, 365, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(1113, true, 2)
                });

        testDriver.advanceWallClockTime(Duration.ofSeconds(10));

        var entityLabels = entityLabelsOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(1, entityLabels.size());
        assertEquals("Foo, Bar", entityLabels.entrySet().stream().findFirst().get().getValue().getLabel());

        // mark Foo as deleted
        sendLabelEdge(1, 365, "i1", 1113, true, 1f, "", "i2", "Foo", "en", true, true);

        entityLabels = entityLabelsOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(1, entityLabels.size());
        assertEquals("Bar", entityLabels.entrySet().stream().findFirst().get().getValue().getLabel());

        // mark Bar as deleted
        sendLabelEdge(1, 365, "i1", 1113, true, 1.2f, "", "i3", "Bar", "en", true, true);

        var list = entityLabelsOutputTopic.readRecordsToList();

        // test label edge by source
        assertEquals(1, list.size());
        assertNull(list.get(0).value());
    }

    /**
     * This should never happen in reality
     */
    @Test
    public void createEntityLabelEdgeTombstone() {
        // Publish test input
        var k = sendLabelEdge(1, 365, "i1", 1113, true, 1f, "", "i2", "Foo", "en", true, false);
        sendConfig(1, 365, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(1113, true, 1)
                });

        testDriver.advanceWallClockTime(Duration.ofSeconds(10));

        var entityLabels = entityLabelsOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(1, entityLabels.size());
        assertEquals("Foo", entityLabels.entrySet().stream().findFirst().get().getValue().getLabel());

        // send tombstone
        this.labelEdgeBySourceInputTopic.pipeInput(k, null);

    }

    @Test
    public void createEntityLabelChangeConfig() {
        // Publish test input
        sendLabelEdge(1, 365, "i1", 1113, true, 1f, "", "i2", "Foo", "en", true, false);
        sendLabelEdge(1, 365, "i1", 1113, true, 1.2f, "", "i3", "Bar", "en", true, false);
        sendConfig(1, 365, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(1113, true, 2)
                });

        testDriver.advanceWallClockTime(Duration.ofSeconds(10));

        var entityLabels = entityLabelsOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(1, entityLabels.size());
        assertEquals("Foo, Bar", entityLabels.entrySet().stream().findFirst().get().getValue().getLabel());

        sendConfig(1, 365, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(1113, true, 1)
                });

        // trigger scheduled process
        testDriver.advanceWallClockTime(Duration.ofSeconds(1));

        entityLabels = entityLabelsOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(1, entityLabels.size());
        assertEquals("Foo", entityLabels.entrySet().stream().findFirst().get().getValue().getLabel());

    }

    @Test
    public void createEntityLabelMarkProjectConfigAsDeleted() {
        // Publish test input
        sendLabelEdge(1, 365, "i1", 1113, true, 2f, "", "i2", "Foo", "en", true, false);
        sendLabelEdge(1, 365, "i1", 1113, true, 1.2f, "", "i3", "Bar", "en", true, false);

        // send project config
        sendConfig(1, 365, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(1113, true, 1)
                });
        // send default config
        sendConfig(DEFAULT_PROJECT.get(), 365, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(1113, true, 2)
                });

        testDriver.advanceWallClockTime(Duration.ofSeconds(1));

        var entityLabels = entityLabelsOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(1, entityLabels.size());
        assertEquals("Bar", entityLabels.entrySet().stream().findFirst().get().getValue().getLabel());


        // mark project config as deleted
        sendConfig(1, 365, 2L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(1113, true, 1)
                }, true);

        // trigger scheduled process
        testDriver.advanceWallClockTime(Duration.ofSeconds(1));

        entityLabels = entityLabelsOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(1, entityLabels.size());
        assertEquals("Bar, Foo", entityLabels.entrySet().stream().findFirst().get().getValue().getLabel());

    }

    @Test
    public void createEntityLabelMarkDefaultConfigAsDeleted() {
        // Publish test input
        sendLabelEdge(1, 365, "i1", 1113, true, 2f, "", "i2", "Foo", "en", true, false);
        sendLabelEdge(1, 365, "i1", 1113, true, 1.2f, "", "i3", "Bar", "en", true, false);


        // send default config
        sendConfig(DEFAULT_PROJECT.get(), 365, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(1113, true, 2)
                });

        testDriver.advanceWallClockTime(Duration.ofSeconds(1));

        var entityLabels = entityLabelsOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(1, entityLabels.size());
        assertEquals("Bar, Foo", entityLabels.entrySet().stream().findFirst().get().getValue().getLabel());


        // mark default config as deleted
        sendConfig(DEFAULT_PROJECT.get(), 365, 2L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(1113, true, 1)
                }, true);

        // trigger scheduled process
        testDriver.advanceWallClockTime(Duration.ofSeconds(1));

        var list = entityLabelsOutputTopic.readRecordsToList();

        // test label is deleted
        assertEquals(1, list.size());
        assertEquals("i1", list.get(0).key().getEntityId());
        assertNull(list.get(0).value());

    }

    public String sendLabelEdge(Integer project_id, Integer source_class_id, String source_id, Integer property_id, Boolean is_outgoing, Float ord_num, String modified_at, String target_id, String target_label, String target_label_language, Boolean target_is_in_project, Boolean deleted) {
        var v = new LabelEdge(project_id,
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
        var k = Fn.createLabelEdgeSourceKey(v);
        this.labelEdgeBySourceInputTopic.pipeInput(k, v);
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
