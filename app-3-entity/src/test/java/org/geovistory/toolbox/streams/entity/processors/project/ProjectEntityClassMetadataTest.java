package org.geovistory.toolbox.streams.entity.processors.project;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectEntityClassMetadataTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectEntityClassMetadataTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectEntityKey, ProjectEntityValue> projectEntityTopic;
    private TestInputTopic<OntomeClassKey, OntomeClassMetadataValue> ontomeClassMetadataTopic;
    private TestOutputTopic<ProjectEntityKey, ProjectEntityClassMetadataValue> outputTopic;

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
        var communityClassLabel = new ProjectEntityClassMetadata(avroSerdes, registerInputTopic, outputTopicNames);
        communityClassLabel.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        projectEntityTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectEntity(),
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityValue().serializer());

        ontomeClassMetadataTopic = testDriver.createInputTopic(
                inputTopicNames.getOntomeClassMetadata(),
                avroSerdes.OntomeClassKey().serializer(),
                avroSerdes.OntomeClassMetadataValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectEntityClassMetadata(),
                avroSerdes.ProjectEntityKey().deserializer(),
                avroSerdes.ProjectEntityClassMetadataValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }


    @Test
    void testProjectEntityMetadata() {

        var entityId = "i1";
        var projectId = 2;
        var classId = 3;
        var parentClasses = List.of(1, 2, 3);
        var ancestorClasses = List.of(4, 5, 6);


        // add an entity
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(projectId).build();
        var vE = ProjectEntityValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setClassId(3).build();
        projectEntityTopic.pipeInput(kE, vE);

        // add ontome class metadata
        var kS = OntomeClassKey.newBuilder().setClassId(classId).build();
        var vS = OntomeClassMetadataValue.newBuilder()
                .setAncestorClasses(ancestorClasses)
                .setParentClasses(parentClasses)
                .build();
        ontomeClassMetadataTopic.pipeInput(kS, vS);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectEntityKey.newBuilder()
                .setEntityId(entityId)
                .setProjectId(projectId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getParentClasses()).contains(1, 2, 3);
        assertThat(record.getAncestorClasses()).contains(4, 5, 6);

    }


}
