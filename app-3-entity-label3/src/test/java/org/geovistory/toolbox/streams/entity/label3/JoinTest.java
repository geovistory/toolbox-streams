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
import org.geovistory.toolbox.streams.avro.EntityLabel;
import org.geovistory.toolbox.streams.avro.LabelEdge;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.entity.label3.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.label3.lib.Fn;
import org.geovistory.toolbox.streams.entity.label3.names.OutputTopicNames;
import org.geovistory.toolbox.streams.testlib.FileRemover;
import org.geovistory.toolbox.streams.testlib.TopologyTestDriverProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
public class JoinTest {

    @Inject
    Topology topology;
    @Inject
    ConfiguredAvroSerde as;
    @Inject
    OutputTopicNames outputTopicNames;
    @ConfigProperty(name = "kafka-streams.state.dir")
    public String stateDir;
    TopologyTestDriver testDriver;
    TestInputTopic<ProjectEntityKey, EntityLabel> labelInputTopic;
    TestInputTopic<String, LabelEdge> edgeInputTopic;
    TestOutputTopic<String, LabelEdge> outputTopic;

    @BeforeEach
    public void setUp() {
        testDriver = new TopologyTestDriver(topology);
        labelInputTopic = testDriver.createInputTopic(
                outputTopicNames.entityLabels(),
                as.kS(), as.vS()
        );
        edgeInputTopic = testDriver.createInputTopic(
                outputTopicNames.labelEdgeByTarget(),
                Serdes.String().serializer(), as.vS()
        );
        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.labelEdgeBySource(),
                Serdes.String().deserializer(), as.vD()
        );

    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
        FileRemover.removeDir(this.stateDir);

    }

    @Test
    public void joinLabels() {
        // Publish test input

        sendLabel(1, "i2", "Person 1", "de");
        sendEdge(1, 21, "i1", 1111, false, 1f, "", "i2", null, "", true, false);


        var edgesBySource = outputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(1, edgesBySource.size());
        assertEquals("Person 1", edgesBySource.entrySet().stream().findFirst().get().getValue().getTargetLabel());

    }

    @Test
    public void joinLabelsTheOtherWay() {
        // Publish test input

        sendEdge(1, 21, "i1", 1111, false, 1f, "", "i2", null, "", true, false);
        sendLabel(1, "i2", "Person 1", "de");


        var edgesBySource = outputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(1, edgesBySource.size());
        assertEquals("Person 1", edgesBySource.entrySet().stream().findFirst().get().getValue().getTargetLabel());

    }

    @Test
    public void joinLabelsEntityLabelTombstone() {
        // Publish test input

        sendEdge(1, 21, "i1", 1111, false, 1f, "", "i2", null, "", true, false);
        sendLabel(1, "i2", "Person 1", "de");


        var edgesBySource = outputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(1, edgesBySource.size());
        assertEquals("Person 1", edgesBySource.entrySet().stream().findFirst().get().getValue().getTargetLabel());

        // send tombstone
        sendLabelTombstone(1, "i2");

        edgesBySource = outputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(1, edgesBySource.size());
        assertEquals(true, edgesBySource.entrySet().stream().findFirst().get().getValue().getDeleted());

    }

    @Test
    public void joinLabelsEdgeTombstone() {
        // Publish test input

        var k = sendEdge(1, 21, "i1", 1111, false, 1f, "", "i2", null, "", true, false);
        sendLabel(1, "i2", "Person 1", "de");


        var edgesBySource = outputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(1, edgesBySource.size());
        assertEquals("Person 1", edgesBySource.entrySet().stream().findFirst().get().getValue().getTargetLabel());

        // send edge tombstone
        this.edgeInputTopic.pipeInput(k, null);

        edgesBySource = outputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(0, edgesBySource.size());
    }

    @Test
    public void joinLabelsEdgeDeleted() {
        // Publish test input

        sendEdge(1, 21, "i1", 1111, false, 1f, "", "i2", null, "", true, false);
        sendLabel(1, "i2", "Person 1", "de");


        var edgesBySource = outputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(1, edgesBySource.size());
        assertEquals("Person 1", edgesBySource.entrySet().stream().findFirst().get().getValue().getTargetLabel());

        // send edge deleted
        sendEdge(1, 21, "i1", 1111, false, 1f, "", "i2", null, "", true, true);

        edgesBySource = outputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(1, edgesBySource.size());
        assertEquals(true, edgesBySource.entrySet().stream().findFirst().get().getValue().getDeleted());
    }

    @Test
    public void joinCommunityLabel() {
        // Publish test input

        sendLabel(0, "i2", "Person 1", "de");
        sendEdge(1, 21, "i1", 1111, false, 1f, "", "i2", null, "", false, false);


        var edgesBySource = outputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(1, edgesBySource.size());
        assertEquals("Person 1", edgesBySource.entrySet().stream().findFirst().get().getValue().getTargetLabel());

    }


    @Test
    public void doNotJoinCommunityLabel() {
        // Publish test input

        sendLabel(0, "i2", "Person 1", "de");
        sendEdge(1, 21, "i1", 1111, false, 1f, "", "i2", null, "", true, false);


        var edgesBySource = outputTopic.readKeyValuesToMap();

        // test label edge by source
        assertEquals(0, edgesBySource.size());

    }


    public String sendEdge(Integer project_id, Integer source_class_id, String source_id, Integer property_id, Boolean is_outgoing, Float ord_num, String modified_at, String target_id, String target_label, String target_label_language, Boolean target_is_in_project, Boolean deleted) {
        var v = LabelEdge.newBuilder()
                .setCommunityToolbox(true)
                .setCommunityPublic(true)
                .setProjectPublic(true)
                .setProjectId(project_id)
                .setSourceClassId(source_class_id)
                .setSourceId(source_id)
                .setPropertyId(property_id)
                .setIsOutgoing(is_outgoing)
                .setOrdNum(ord_num)
                .setModifiedAt(modified_at)
                .setTargetId(target_id)
                .setTargetLabel(target_label)
                .setTargetLabelLanguage(target_label_language)
                .setTargetIsInProject(target_is_in_project)
                .setDeleted(deleted).build();
        var k = Fn.createLabelEdgeSourceKey(v);
        this.edgeInputTopic.pipeInput(k, v);
        return k;
    }


    public void sendLabel(Integer project_id, String entity_id, String label, String language) {
        var k = new ProjectEntityKey(project_id, entity_id);
        var v = new EntityLabel(label, language);
        this.labelInputTopic.pipeInput(k, v);
    }

    public void sendLabelTombstone(Integer project_id, String entity_id) {
        var k = new ProjectEntityKey(project_id, entity_id);
        this.labelInputTopic.pipeInput(k, null);
    }


}
