package org.geovistory.toolbox.streams.entity.label.processors.project;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityVisibilityValue;
import org.geovistory.toolbox.streams.entity.label.*;
import org.geovistory.toolbox.streams.entity.label.processsors.project.ProjectEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectEntityTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectEntityTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectEntityKey, ProjectEntityVisibilityValue> projectEntityVisibilityTopic;
    private TestOutputTopic<ProjectEntityKey, ProjectEntityValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");

        var builderSingleton = new BuilderSingleton();
        var avroSerdes = new AvroSerdes();
        avroSerdes.QUARKUS_KAFKA_STREAMS_SCHEMA_REGISTRY_URL = MOCK_SCHEMA_REGISTRY_URL;
        var inputTopicNames = new InputTopicNames();
        var outputTopicNames = new OutputTopicNames();
        var registerInputTopic = new RegisterInputTopic(avroSerdes, builderSingleton, inputTopicNames);
        var registerInnerTopic = new RegisterInnerTopic(avroSerdes, builderSingleton, outputTopicNames);
        var projectEntity = new ProjectEntity(avroSerdes, registerInputTopic, registerInnerTopic, outputTopicNames, builderSingleton);
        projectEntity.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        projectEntityVisibilityTopic = testDriver.createInputTopic(
                outputTopicNames.projectEntityVisibility(),
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityVisibilityValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectEntity(),
                avroSerdes.ProjectEntityKey().deserializer(),
                avroSerdes.ProjectEntityValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testProjection() {
        var entityId = "i1";
        var projectId = 2;

        // add entity
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(projectId).build();
        var vE = ProjectEntityVisibilityValue.newBuilder()
                .setEntityId(entityId).setProjectId(projectId)
                .setClassId(3)
                .setCommunityVisibilityToolbox(true)
                .setCommunityVisibilityDataApi(true)
                .setCommunityVisibilityWebsite(true)
                .setDeleted$1(false)
                .build();
        projectEntityVisibilityTopic.pipeInput(kE, vE);

        // modify irrelevant part
        vE.setCommunityVisibilityWebsite(false);
        projectEntityVisibilityTopic.pipeInput(kE, vE);

        // add second entity
        kE = ProjectEntityKey.newBuilder().setEntityId("i2").setProjectId(projectId).build();
        vE = ProjectEntityVisibilityValue.newBuilder()
                .setEntityId("i2").setProjectId(projectId)
                .setClassId(3)
                .setCommunityVisibilityToolbox(true)
                .setCommunityVisibilityDataApi(true)
                .setCommunityVisibilityWebsite(true)
                .setDeleted$1(false)
                .build();
        projectEntityVisibilityTopic.pipeInput(kE, vE);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readRecordsToList();
        assertThat(outRecords).hasSize(2);

        var record = outRecords.get(0);
        assertThat(record.value().getDeleted$1()).isEqualTo(false);
    }

    @Test
    void testTombstone() {
        var entityId = "i1";
        var projectId = 2;

        // add entity
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(projectId).build();
        var vE = ProjectEntityVisibilityValue.newBuilder()
                .setEntityId(entityId).setProjectId(projectId)
                .setClassId(3)
                .setCommunityVisibilityToolbox(true)
                .setCommunityVisibilityDataApi(true)
                .setCommunityVisibilityWebsite(true)
                .setDeleted$1(false)
                .build();
        projectEntityVisibilityTopic.pipeInput(kE, vE);

        // add tombstone
        projectEntityVisibilityTopic.pipeInput(kE, null);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readRecordsToList();
        assertThat(outRecords).hasSize(2);

        var record = outRecords.get(1);
        assertThat(record.value().getDeleted$1()).isEqualTo(true);
    }

}
