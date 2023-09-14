package org.geovistory.toolbox.streams.rdf.processors.project;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.rdf.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectEntityRdfsLabelTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectEntityRdfsLabelTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectEntityKey, ProjectEntityLabelValue> projectEntityLabel;
    private TestOutputTopic<ProjectRdfKey, ProjectRdfValue> outputTopic;

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
        var projectEntityRdfsLabel = new ProjectEntityRdfsLabel(avroSerdes, registerInputTopic, outputTopicNames);
        projectEntityRdfsLabel.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        projectEntityLabel = testDriver.createInputTopic(
                inputTopicNames.getProjectEntityLabel(),
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityLabelValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectRdf(),
                avroSerdes.ProjectRdfKey().deserializer(),
                avroSerdes.ProjectRdfValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    /**
     * The goal of this test is to check if the output is not empty
     */
    @Test
    void testOutputIsNotEmpty() {

        var projectId = 1;
        var entityId = "1";
        var label = "test";

        var k = ProjectEntityKey.newBuilder().setProjectId(projectId).setEntityId(entityId).build();
        var v = ProjectEntityLabelValue.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .setLabel(label)
                .setLabelSlots(List.of(label))
                .build();
        projectEntityLabel.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
    }
    /**
     * The goal of this test is to check if the output has the correct size
     * For each entity, one triple for the label should be added (size = 1)
     */
    @Test
    void testSizeOfOutput() {

        var projectId = 1;
        var entityId = "1";
        var label = "test";

        var k = ProjectEntityKey.newBuilder().setProjectId(projectId).setEntityId(entityId).build();
        var v = ProjectEntityLabelValue.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .setLabel(label)
                .setLabelSlots(List.of(label))
                .build();
        projectEntityLabel.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
    }

    /**
     * The goal of this test is to check if the operation value (insert or delete) is correctly set in the output topic
     */
    @Test
    void testOperationValue() {

        var projectId = 1;
        var entityId = "1";
        var label = "test";
        var newLabel = "test2";

        var k = ProjectEntityKey.newBuilder().setProjectId(projectId).setEntityId(entityId).build();
        var v = ProjectEntityLabelValue.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .setLabel(label)
                .setLabelSlots(List.of(label))
                .build();
        projectEntityLabel.pipeInput(k, v);

        var outRecords = outputTopic.readKeyValuesToMap();

        var expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/1> <http://www.w3.org/2000/01/rdf-schema#label> \"test\"@^^<http://www.w3.org/2001/XMLSchema#string> .")
                .build();

        var record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        var expectedKey2 = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/1> <http://www.w3.org/2000/01/rdf-schema#label> \"test\"@^^<http://www.w3.org/2001/XMLSchema#string> .")
                .build();

        var record2 = outRecords.get(expectedKey2);
        assertThat(record2.getOperation()).isEqualTo(Operation.insert);

        var v2 = ProjectEntityLabelValue.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .setLabel(label)
                .setLabelSlots(List.of(label))
                .build();
        projectEntityLabel.pipeInput(k, v2);

        var v3 = ProjectEntityLabelValue.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .setLabel(newLabel)
                .setLabelSlots(List.of(newLabel))
                .build();
        projectEntityLabel.pipeInput(k, v3);

        outRecords = outputTopic.readKeyValuesToMap();

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/1> <http://www.w3.org/2000/01/rdf-schema#label> \"test\"@^^<http://www.w3.org/2001/XMLSchema#string> .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/1> <http://www.w3.org/2000/01/rdf-schema#label> \"test2\"@^^<http://www.w3.org/2001/XMLSchema#string> .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);
    }
}