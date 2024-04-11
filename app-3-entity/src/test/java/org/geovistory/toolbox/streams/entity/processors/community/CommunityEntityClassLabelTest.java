package org.geovistory.toolbox.streams.entity.processors.community;


import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.kafka.streams.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.I;
import org.geovistory.toolbox.streams.entity.InputTopicNames;
import org.geovistory.toolbox.streams.entity.OutputTopicNames;
import org.geovistory.toolbox.streams.testlib.FileRemover;
import org.geovistory.toolbox.streams.testlib.TopologyTestDriverProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
class CommunityEntityClassLabelTest {
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
    private TestInputTopic<ProjectEntityKey, ProjectEntityValue> communityEntityTopic;
    private TestInputTopic<OntomeClassLabelKey, CommunityClassLabelValue> communityClassLabelTopic;
    private TestOutputTopic<ProjectEntityKey, ProjectEntityClassLabelValue> outputTopic;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");

        testDriver = new TopologyTestDriver(topology, props);


        communityClassLabelTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityClassLabel(),
                as.kS(),
                as.vS());

        communityEntityTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityEntity(),
                as.kS(),
                as.vS());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.communityEntityClassLabel(),
                as.kD(),
                as.vD());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
        FileRemover.removeDir(this.stateDir);
    }


    @Test
    void testCommunityEntityClassLabel() {

        var entityId = "i1";
        var classId = 3;
        var classLabel = "my_class";

        // add an entity
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(0).build();
        var vE = ProjectEntityValue.newBuilder().setEntityId(entityId).setProjectId(0).setDeleted$1(false).setClassId(3).build();
        communityEntityTopic.pipeInput(kE, vE);

        // add a class label
        var kS = OntomeClassLabelKey.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.EN.get())
                .build();
        var vS = CommunityClassLabelValue.newBuilder()
                .setLabel(classLabel).build();
        communityClassLabelTopic.pipeInput(kS, vS);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kE);
        assertThat(record.getClassLabel()).isEqualTo(classLabel);

    }


    @Test
    void testDeletion() {

        var entityId = "i1";
        var classId = 3;
        var classLabel = "my_class";

        // add an entity
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(0).build();
        var vE = ProjectEntityValue.newBuilder().setEntityId(entityId).setProjectId(0).setDeleted$1(false).setClassId(3).build();
        communityEntityTopic.pipeInput(kE, vE);
        vE.setDeleted$1(true);
        communityEntityTopic.pipeInput(kE, vE);

        // add a class label
        var kS = OntomeClassLabelKey.newBuilder().setClassId(classId)
                .setLanguageId(I.EN.get())
                .build();
        var vS = CommunityClassLabelValue.newBuilder()
                .setLabel(classLabel).build();
        communityClassLabelTopic.pipeInput(kS, vS);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kE);
        assertThat(record.getDeleted$1()).isEqualTo(true);

    }
}
