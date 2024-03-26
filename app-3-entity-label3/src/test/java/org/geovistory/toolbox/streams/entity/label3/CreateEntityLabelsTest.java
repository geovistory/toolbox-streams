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
    TestInputTopic<String, LabelEdge> labelEdgeByTargetInputTopic;
    TestOutputTopic<ProjectEntityKey, EntityLabel> entityLabelsOutputTopic;
    TestOutputTopic<ProjectEntityLangKey, EntityLabel> entityLanguageLabelsOutputTopic;

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
        entityLabelsOutputTopic = testDriver.createOutputTopic(
                outputTopicNames.entityLabels(),
                as.kD(), as.vD()
        );
        entityLanguageLabelsOutputTopic = testDriver.createOutputTopic(
                outputTopicNames.entityLanguageLabels(),
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
        assertEquals(2, entityLabels.size());
        assertEquals("Foo", entityLabels.get(new ProjectEntityKey(1, "i1")).getLabel());

    }

    // Assure, statement order is respected also with large ord nums (assert that 10 comes after 2)
    @Test
    public void test() {
        // Publish test input
        sendConfig(1, 365, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(1113, true, 3)
                });
        sendLabelEdge(1, 365, "i1", 1113, true, 10f, "", "i2", "Bar", "en", true, false);
        sendLabelEdge(1, 365, "i1", 1113, true, 0.32f, "", "i2", "Foo", "en", true, false);
        sendLabelEdge(1, 365, "i1", 1113, true, 11f, "", "i2", "Baz", "en", true, false);


        var entityLabels = entityLabelsOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(2, entityLabels.size());
        assertEquals("Foo, Bar, Baz", entityLabels.get(new ProjectEntityKey(1, "i1")).getLabel());

    }

    // Assure, statement order is respected also with timestamps (assert that newest comes first)
    @Test
    public void testOrderByNewestFirst() {
        // Publish test input
        sendConfig(1, 365, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(1113, true, 6)
                });
        sendLabelEdge(1, 365, "i1", 1113, true, 10f, "2020-02-26T10:38:11.262940Z", "i2", "Bar", "en", true, false);
        sendLabelEdge(1, 365, "i1", 1113, true, 0.32f, "2020-02-26T10:38:11.262940Z", "i2", "Foo", "en", true, false);
        sendLabelEdge(1, 365, "i1", 1113, true, null, "2016-02-26T10:38:11.262940Z", "i2", "Baz", "en", true, false);
        sendLabelEdge(1, 365, "i1", 1113, true, null, "2140-02-26T10:38:11.262940Z", "i2", "Li", "en", true, false);
        sendLabelEdge(1, 365, "i1", 1113, true, null, "2024-02-26T10:38:11.262940Z", "i2", "La", "en", true, false);
        sendLabelEdge(1, 365, "i1", 1113, true, null, "2023-02-26T10:38:11.262940Z", "i2", "Lo", "en", true, false);


        var entityLabels = entityLabelsOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(2, entityLabels.size());
        assertEquals("Foo, Bar, Li, La, Lo, Baz", entityLabels.get(new ProjectEntityKey(1, "i1")).getLabel());

    }


    // Test join of community entity label
    @Test
    public void testJoinCommunityEntityLabel() {
        // Publish test input
        sendConfig(DEFAULT_PROJECT.get(), 2, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(3, true, 1)
                });
        sendConfig(DEFAULT_PROJECT.get(), 3, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(4, true, 1)
                });
        sendLabelEdge(1, 2, "i1", 3, true, 10f, "", "i2", null, null, false, false, false);
        sendLabelEdge(2, 3, "i2", 4, true, 1f, "", "i3", "Foo", "en", true, false);


        var entityLabels = entityLabelsOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(4, entityLabels.size());
        assertEquals("Foo", entityLabels.get(new ProjectEntityKey(0, "i2")).getLabel());
        assertEquals("Foo", entityLabels.get(new ProjectEntityKey(2, "i2")).getLabel());
        assertEquals("Foo", entityLabels.get(new ProjectEntityKey(0, "i1")).getLabel());
        assertEquals("Foo", entityLabels.get(new ProjectEntityKey(1, "i1")).getLabel());

        // test adding target entity to project
        sendLabelEdge(1, 2, "i1", 3, true, 10f, "", "i2", null, null, true, false, false);

        // assert the label is deleted (tombstone)
        var list = entityLabelsOutputTopic.readRecordsToList();
        assertEquals(2, list.size());
        var firstTombstone = list.get(0);
        assertEquals(new ProjectEntityKey(1, "i1"), firstTombstone.key());
        assertNull(firstTombstone.value());
        var secondTombstone = list.get(1);
        assertEquals(new ProjectEntityKey(0, "i1"), secondTombstone.key());
        assertNull(secondTombstone.value());
    }

    // Test community label is most frequently used label
    @Test
    public void testCommunityLabelIsMostFrequentLabel() {
        // Publish test input
        sendConfig(DEFAULT_PROJECT.get(), 3, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(4, true, 1)
                });
        sendLabelEdge(1, 3, "i2", 4, true, 1f, "", "i3", "Foo", "en", true, false);
        sendLabelEdge(2, 3, "i2", 4, true, 1f, "", "i3", "Bar", "en", true, false);
        sendLabelEdge(3, 3, "i2", 4, true, 1f, "", "i3", "Baz", "en", true, false);
        sendLabelEdge(4, 3, "i2", 4, true, 1f, "", "i3", "Bar", "en", true, false);


        var entityLabels = entityLabelsOutputTopic.readKeyValuesToMap();

        assertEquals("Bar", entityLabels.get(new ProjectEntityKey(0, "i2")).getLabel());

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
        sendLabelEdge(2, 3, "i2", 4, true, 1f, "", "i3", "Bar", "fr", true, false);
        sendLabelEdge(3, 3, "i2", 4, true, 1f, "", "i3", "Baz", "de", true, false);
        sendLabelEdge(4, 3, "i2", 4, true, 1f, "", "i3", "Bar", "fr", true, false);
        sendLabelEdge(4, 3, "i2", 4, true, 1f, "", "i3", "Bar 2", "fr", true, false);


        var entityLabels = entityLanguageLabelsOutputTopic.readKeyValuesToMap();
        assertEquals(3, entityLabels.size());

        assertEquals("Foo", entityLabels.get(new ProjectEntityLangKey(0, "i2", "en")).getLabel());
        assertEquals("Bar", entityLabels.get(new ProjectEntityLangKey(0, "i2", "fr")).getLabel());
        assertEquals("Baz", entityLabels.get(new ProjectEntityLangKey(0, "i2", "de")).getLabel());

    }

    @Test
    public void trimSpaces() {
        // Publish test input
        sendConfig(1, 365, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(1113, true, 1)
                });
        sendLabelEdge(1, 365, "i1", 1113, true, 1f, "", "i2", " Foo ", "en", true, false);


        var entityLabels = entityLabelsOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(2, entityLabels.size());
        assertEquals("Foo", entityLabels.get(new ProjectEntityKey(1, "i1")).getLabel());
        assertEquals("Foo", entityLabels.get(new ProjectEntityKey(0, "i1")).getLabel());

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
        assertEquals(2, entityLabels.size());
        assertEquals("Foo", entityLabels.get(new ProjectEntityKey(1, "i1")).getLabel());
        assertEquals("Foo", entityLabels.get(new ProjectEntityKey(0, "i1")).getLabel());

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
        assertEquals(2, entityLabels.size());
        assertEquals("Foo, Bar", entityLabels.get(new ProjectEntityKey(0, "i1")).getLabel());
        assertEquals("Foo, Bar", entityLabels.get(new ProjectEntityKey(1, "i1")).getLabel());

        // mark Foo as deleted
        sendLabelEdge(1, 365, "i1", 1113, true, 1f, "", "i2", "Foo", "en", true, true);

        entityLabels = entityLabelsOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(2, entityLabels.size());
        assertEquals("Bar", entityLabels.get(new ProjectEntityKey(0, "i1")).getLabel());
        assertEquals("Bar", entityLabels.get(new ProjectEntityKey(1, "i1")).getLabel());

        // mark Bar as deleted
        sendLabelEdge(1, 365, "i1", 1113, true, 1.2f, "", "i3", "Bar", "en", true, true);

        var list = entityLabelsOutputTopic.readRecordsToList();

        // test deletion
        assertEquals(2, list.size());
        var firstTombstone = list.get(0);
        assertEquals(new ProjectEntityKey(1, "i1"), firstTombstone.key());
        assertNull(firstTombstone.value());
        var secondTombstone = list.get(1);
        assertEquals(new ProjectEntityKey(0, "i1"), secondTombstone.key());
        assertNull(secondTombstone.value());
    }

    /**
     * This should never happen in reality but not throw an error
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
        assertEquals(2, entityLabels.size());
        assertEquals("Foo", entityLabels.get(new ProjectEntityKey(0, "i1")).getLabel());
        assertEquals("Foo", entityLabels.get(new ProjectEntityKey(1, "i1")).getLabel());

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
        assertEquals(2, entityLabels.size());
        assertEquals("Foo, Bar", entityLabels.get(new ProjectEntityKey(0, "i1")).getLabel());
        assertEquals("Foo, Bar", entityLabels.get(new ProjectEntityKey(1, "i1")).getLabel());

        sendConfig(1, 365, 2L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(1113, true, 1)
                });

        // trigger scheduled process
        testDriver.advanceWallClockTime(Duration.ofSeconds(1));

        entityLabels = entityLabelsOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(2, entityLabels.size());
        assertEquals("Foo", entityLabels.get(new ProjectEntityKey(0, "i1")).getLabel());
        assertEquals("Foo", entityLabels.get(new ProjectEntityKey(1, "i1")).getLabel());

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
        assertEquals(2, entityLabels.size());
        assertEquals("Bar", entityLabels.get(new ProjectEntityKey(0, "i1")).getLabel());
        assertEquals("Bar", entityLabels.get(new ProjectEntityKey(1, "i1")).getLabel());

        // mark project config as deleted
        sendConfig(1, 365, 2L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(1113, true, 1)
                }, true);

        // trigger scheduled process
        testDriver.advanceWallClockTime(Duration.ofSeconds(1));

        entityLabels = entityLabelsOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(2, entityLabels.size());
        assertEquals("Bar, Foo", entityLabels.get(new ProjectEntityKey(0, "i1")).getLabel());
        assertEquals("Bar, Foo", entityLabels.get(new ProjectEntityKey(1, "i1")).getLabel());

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
        assertEquals(2, entityLabels.size());
        assertEquals("Bar, Foo", entityLabels.get(new ProjectEntityKey(0, "i1")).getLabel());
        assertEquals("Bar, Foo", entityLabels.get(new ProjectEntityKey(1, "i1")).getLabel());


        // mark default config as deleted
        sendConfig(DEFAULT_PROJECT.get(), 365, 2L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(1113, true, 1)
                }, true);

        // trigger scheduled process
        testDriver.advanceWallClockTime(Duration.ofSeconds(1));

        var list = entityLabelsOutputTopic.readRecordsToList();

        // test deletion
        assertEquals(2, list.size());
        var firstTombstone = list.get(0);
        assertEquals(new ProjectEntityKey(1, "i1"), firstTombstone.key());
        assertNull(firstTombstone.value());
        var secondTombstone = list.get(1);
        assertEquals(new ProjectEntityKey(0, "i1"), secondTombstone.key());
        assertNull(secondTombstone.value());

    }

    // If a project configures the labels of two classes to depend on each other, we create an infinite
    // label. Assure, this is prevented by a max length of 100 chars per entity label.
    @Test
    public void testMaxLengthForSelfReferencingLabels() {
        // Publish test input
        sendConfig(1, 1, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(1, true, 1)
                });
        sendConfig(1, 2, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(2, true, 1),
                        new EntityLabelConfigPartField(3, false, 1)
                });
        sendConfig(1, 3, 1L,
                new EntityLabelConfigPartField[]{
                        new EntityLabelConfigPartField(3, true, 1)
                });
        sendLabelEdge(1, 1, "i2", 1, true, 1f, "", "i1", "Foo Bar Baz Baa Boo Bee", "en", true, false);
        sendLabelEdge(1, 2, "i3", 2, true, 1f, "", "i2", null, null, true, false, false);
        sendLabelEdge(1, 2, "i3", 3, false, 1f, "", "i4", null, null, true, false, false);
        sendLabelEdge(1, 3, "i4", 3, true, 1f, "", "i3", null, null, true, false, false);


        var entityLabels = entityLabelsOutputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(6, entityLabels.size());
        assertEquals(100, entityLabels.get(new ProjectEntityKey(1, "i3")).getLabel().length());

    }


    public String sendLabelEdge(Integer project_id, Integer source_class_id, String source_id, Integer property_id, Boolean is_outgoing, Float ord_num, String modified_at, String target_id, String target_label, String target_label_language, Boolean target_is_in_project, Boolean deleted) {
        return sendLabelEdge(project_id,
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
