package org.geovistory.toolbox.streams.entity.label.processors.community;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.CommunityEntityKey;
import org.geovistory.toolbox.streams.avro.CommunityEntityValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityVisibilityValue;
import org.geovistory.toolbox.streams.entity.label.*;
import org.geovistory.toolbox.streams.entity.label.processsors.community.CommunityToolboxEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CommunityToolboxEntityTest {

    private static final String SCHEMA_REGISTRY_SCOPE = CommunityToolboxEntityTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectEntityKey, ProjectEntityVisibilityValue> projectEntityTopic;
    private TestOutputTopic<CommunityEntityKey, CommunityEntityValue> outputTopic;

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
        var communityToolboxEntity = new CommunityToolboxEntity(avroSerdes, registerInputTopic, registerInnerTopic, outputTopicNames);
        communityToolboxEntity.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);


        projectEntityTopic = testDriver.createInputTopic(
                outputTopicNames.projectEntityVisibility(),
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityVisibilityValue().serializer());


        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.communityToolboxEntity(),
                avroSerdes.CommunityEntityKey().deserializer(),
                avroSerdes.CommunityEntityValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }


    @Test
    public void testAddEntityToTwoProjects() {

        var key1 = ProjectEntityKey.newBuilder()
                .setEntityId("i1").setProjectId(1).build();
        var value1 = ProjectEntityVisibilityValue.newBuilder()
                .setEntityId("i1")
                .setProjectId(1)
                .setClassId(1)
                .setCommunityVisibilityToolbox(true)
                .setCommunityVisibilityDataApi(true)
                .setCommunityVisibilityWebsite(true)
                .setDeleted$1(false)
                .build();

        // add to project 1
        projectEntityTopic.pipeInput(key1, value1);

        // add to project 2
        value1.setProjectId(2);
        projectEntityTopic.pipeInput(key1, value1);

        // passing the same key-value pair again, should not change the count
        projectEntityTopic.pipeInput(key1, value1);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = CommunityEntityKey.newBuilder()
                .setEntityId("i1")
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getProjectCount()).isEqualTo(2);

    }


    @Test
    public void testRemoveEntityFromProject() {

        var key1 = ProjectEntityKey.newBuilder()
                .setEntityId("i1").setProjectId(1).build();
        var value1 = ProjectEntityVisibilityValue.newBuilder()
                .setEntityId("i1")
                .setProjectId(1)
                .setClassId(1)
                .setCommunityVisibilityToolbox(true)
                .setCommunityVisibilityDataApi(true)
                .setCommunityVisibilityWebsite(true)
                .setDeleted$1(false)
                .build();

        // add to project 1
        projectEntityTopic.pipeInput(key1, value1);

        // add to project 2
        value1.setProjectId(2);
        projectEntityTopic.pipeInput(key1, value1);

        // remove from project 2
        value1.setDeleted$1(true);
        projectEntityTopic.pipeInput(key1, value1);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = CommunityEntityKey.newBuilder()
                .setEntityId("i1")
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getProjectCount()).isEqualTo(1);

    }


    @Test
    public void testEntityNotVisibleForCommunity() {

        var key1 = ProjectEntityKey.newBuilder()
                .setEntityId("i1").setProjectId(1).build();
        var value1 = ProjectEntityVisibilityValue.newBuilder()
                .setEntityId("i1")
                .setProjectId(1)
                .setClassId(1)
                .setCommunityVisibilityToolbox(false)
                .setCommunityVisibilityDataApi(true)
                .setCommunityVisibilityWebsite(true)
                .setDeleted$1(false)
                .build();

        // add to project 1
        projectEntityTopic.pipeInput(key1, value1);

        // add to project 2
        value1.setProjectId(2);
        projectEntityTopic.pipeInput(key1, value1);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = CommunityEntityKey.newBuilder()
                .setEntityId("i1")
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getProjectCount()).isEqualTo(0);

    }


}
