package org.geovistory.toolbox.streams.entity.preview.processors;


import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.kafka.streams.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.CommunityEntityKey;
import org.geovistory.toolbox.streams.avro.EntityPreviewValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.entity.preview.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.preview.InputTopicNames;
import org.geovistory.toolbox.streams.entity.preview.OutputTopicNames;
import org.geovistory.toolbox.streams.testlib.TopologyTestDriverProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
class EntityPreviewTest {
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
    private TestInputTopic<ProjectEntityKey, EntityPreviewValue> projectEntityPreviewTopic;
    private TestInputTopic<CommunityEntityKey, EntityPreviewValue> communityEntityPreviewTopic;
    private TestOutputTopic<ProjectEntityKey, EntityPreviewValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testApplicationId");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        testDriver = new TopologyTestDriver(topology, config);


        projectEntityPreviewTopic = testDriver.createInputTopic(
                outputTopicNames.projectEntityPreview(),
                as.kS(),
                as.vS());

        communityEntityPreviewTopic = testDriver.createInputTopic(
                outputTopicNames.communityEntityPreview(),
                as.kS(),
                as.vS());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.entityPreview(),
                as.kD(),
                as.vD());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }


    @Test
    void testMerge() {

        var kP = ProjectEntityKey.newBuilder().setEntityId("1").setProjectId(2).build();
        var vP = EntityPreviewValue.newBuilder().setEntityId("1")
                .setProject(2)
                .setFkProject(2)
                .setPkEntity(1)
                .setEntityType("")
                .setEntityLabel("")
                .setAncestorClasses("")
                .setParentClasses("")
                .build();
        projectEntityPreviewTopic.pipeInput(kP, vP);

        var kC = CommunityEntityKey.newBuilder().setEntityId("99").build();
        var vC = EntityPreviewValue.newBuilder().setEntityId("99")
                .setProject(0)
                .setFkProject(0)
                .setPkEntity(99)
                .setEntityType("")
                .setEntityLabel("")
                .setAncestorClasses("")
                .setParentClasses("")
                .build();
        communityEntityPreviewTopic.pipeInput(kC, vC);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);

        var record = outRecords.get(kP);
        assertThat(record.getFkProject()).isEqualTo(2);

        kP.setProjectId(0);
        kP.setEntityId("99");
        record = outRecords.get(kP);
        assertThat(record.getFkProject()).isEqualTo(0);


    }

}
