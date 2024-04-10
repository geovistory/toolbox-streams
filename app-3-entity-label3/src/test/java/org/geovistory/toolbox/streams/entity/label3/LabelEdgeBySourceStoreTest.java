package org.geovistory.toolbox.streams.entity.label3;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.LabelEdge;
import org.geovistory.toolbox.streams.entity.label3.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.label3.names.InputTopicNames;
import org.geovistory.toolbox.streams.entity.label3.names.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.label3.stores.LabelEdgeBySourceStore;
import org.geovistory.toolbox.streams.testlib.FileRemover;
import org.geovistory.toolbox.streams.testlib.TopologyTestDriverProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ts.projects.entity_label_config.Key;
import ts.projects.entity_label_config.Value;

import static org.junit.jupiter.api.Assertions.assertNotNull;


@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
public class LabelEdgeBySourceStoreTest {

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
    TestInputTopic<String, LabelEdge> labelEdgeBySourceTopic;
    TestInputTopic<Key, Value> labelConfigInputTopic;


    @BeforeEach
    public void setUp() {
        testDriver = new TopologyTestDriver(topology);
        labelEdgeBySourceTopic = testDriver.createInputTopic(
                outputTopicNames.labelEdgeBySource(),
                Serdes.String().serializer(), as.<LabelEdge>value().serializer()
        );
        labelConfigInputTopic = testDriver.createInputTopic(
                inputTopicNames.proEntityLabelConfig(),
                as.<Key>key().serializer(), as.<Value>value().serializer()
        );

    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
        FileRemover.removeDir(this.stateDir);

    }

    @Test
    public void testStoreUpdater() {
        // Publish test input
        sendLabelEdge();
        assertNotNull(testDriver.getKeyValueStore(LabelEdgeBySourceStore.NAME).all().next());
    }

    public void sendLabelEdge() {
        String k = "k";
        var v = LabelEdge.newBuilder()
                .setSourceCommunityToolbox(true)
                .setSourceCommunityPublic(true)
                .setSourceProjectPublic(true)
                .setEdgeCommunityToolbox(true)
                .setProjectId(0)
                .setSourceClassId(0)
                .setSourceId("0")
                .setPropertyId(0)
                .setIsOutgoing(true)
                .setOrdNum(1f)
                .setModifiedAt("")
                .setTargetId("0")
                .setTargetLabel("Foo")
                .setTargetLabelLanguage("unknown")
                .setTargetIsInProject(true)
                .setDeleted(false)
                .build();
        this.labelEdgeBySourceTopic.pipeInput(k, v);
    }

}
