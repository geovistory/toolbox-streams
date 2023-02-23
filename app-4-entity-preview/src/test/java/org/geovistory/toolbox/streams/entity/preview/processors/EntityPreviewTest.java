package org.geovistory.toolbox.streams.entity.preview.processors;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.CommunityEntityKey;
import org.geovistory.toolbox.streams.avro.EntityPreviewValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.entity.preview.processors.community.CommunityEntityPreview;
import org.geovistory.toolbox.streams.entity.preview.processors.project.ProjectEntityPreview;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class EntityPreviewTest {

    private static final String SCHEMA_REGISTRY_SCOPE = EntityPreviewTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectEntityKey, EntityPreviewValue> projectEntityPreviewTopic;
    private TestInputTopic<CommunityEntityKey, EntityPreviewValue> communityEntityPreviewTopic;
    private TestOutputTopic<ProjectEntityKey, EntityPreviewValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        var nameSupplement = "toolbox";
        Topology topology = EntityPreview.buildStandalone(new StreamsBuilder(), nameSupplement);

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();
        projectEntityPreviewTopic = testDriver.createInputTopic(
                ProjectEntityPreview.output.TOPICS.project_entity_preview,
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.EntityPreviewValue().serializer());

        communityEntityPreviewTopic = testDriver.createInputTopic(
                CommunityEntityPreview.getOutputTopicName(nameSupplement),
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.EntityPreviewValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                EntityPreview.output.TOPICS.entity_preview,
                avroSerdes.ProjectEntityKey().deserializer(),
                avroSerdes.EntityPreviewValue().deserializer());
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
