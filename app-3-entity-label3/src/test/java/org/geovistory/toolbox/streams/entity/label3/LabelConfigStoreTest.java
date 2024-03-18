package org.geovistory.toolbox.streams.entity.label3;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.EntityLabelConfig;
import org.geovistory.toolbox.streams.avro.EntityLabelConfigPart;
import org.geovistory.toolbox.streams.avro.EntityLabelConfigPartField;
import org.geovistory.toolbox.streams.entity.label3.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.label3.names.InputTopicNames;
import org.geovistory.toolbox.streams.entity.label3.stores.GlobalLabelConfigStore;
import org.geovistory.toolbox.streams.testlib.FileRemover;
import org.geovistory.toolbox.streams.testlib.TopologyTestDriverProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ts.projects.entity_label_config.Key;
import ts.projects.entity_label_config.Value;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;


@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
public class LabelConfigStoreTest {

    @Inject
    Topology topology;
    @Inject
    ConfiguredAvroSerde as;
    @Inject
    InputTopicNames inputTopicNames;
    @ConfigProperty(name = "kafka-streams.state.dir")
    public String stateDir;
    TopologyTestDriver testDriver;
    TestInputTopic<Key, Value> labelConfigInputTopic;

    @BeforeEach
    public void setUp() {
        testDriver = new TopologyTestDriver(topology);
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
    public void labelConfigStoreUpdaterTest() {
        // Publish test input
        sendLabelConfig(1, 21);
        assertNotNull(testDriver.getKeyValueStore(GlobalLabelConfigStore.NAME).all().next());
    }

    public void sendLabelConfig(int pid, int classId) {
        var k = Key.newBuilder().setPkEntity(123).build();
        var configJson = EntityLabelConfig.newBuilder()
                .setLabelParts(List.of(
                        EntityLabelConfigPart.newBuilder()
                                .setOrdNum(44)
                                .setField(
                                        EntityLabelConfigPartField.newBuilder()
                                                .setFkProperty(44)
                                                .setIsOutgoing(true)
                                                .setNrOfStatementsInLabel(2)
                                                .build()
                                )
                                .build()
                )).build().toString();
        var v = Value.newBuilder()
                .setConfig(configJson)
                .setFkProject(pid)
                .setFkClass(classId)
                .build();
        this.labelConfigInputTopic.pipeInput(k, v);
    }

}
