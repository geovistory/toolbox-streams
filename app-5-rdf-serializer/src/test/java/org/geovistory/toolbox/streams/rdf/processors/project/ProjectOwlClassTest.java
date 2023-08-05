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

class ProjectOwlClassTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectOwlClassTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectClassLabelKey, ProjectClassLabelValue> projectClassLabel;
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
        var communityClassLabel = new ProjectOwlClass(avroSerdes, registerInputTopic, outputTopicNames);
        communityClassLabel.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        projectClassLabel = testDriver.createInputTopic(
                inputTopicNames.getProjectClassLabel(),
                avroSerdes.ProjectClassLabelKey().serializer(),
                avroSerdes.ProjectClassLabelValue().serializer());

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
        var classId = 123212;
        var label = "test";
        var lang = "en";

        var k = ProjectClassLabelKey.newBuilder().setProjectId(projectId).setClassId(classId).build();
        var v = ProjectClassLabelValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setLabel(label)
                .setLanguageIso(lang)
                .setLanguageId(I.EN.get())
                .setDeleted$1(false)
                .build();
        projectClassLabel.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
    }

    /**
     * The goal of this test is to check if the output has the correct size (should be equals to twice the number of statements in the input topic)
     */
    @Test
    void testSizeOfOutput() {

        var projectId = 1;
        var classId = 123212;
        var label = "test";
        var lang = "en";

        var k = ProjectClassLabelKey.newBuilder().setProjectId(projectId).setClassId(classId).build();
        var v = ProjectClassLabelValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setLabel(label)
                .setLanguageIso(lang)
                .setLanguageId(I.EN.get())
                .setDeleted$1(false)
                .build();
        projectClassLabel.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);
    }

    /**
     * The goal of this test is to check if the operation value (insert or delete) is correctly set in the output topic
     */
    @Test
    void testOperationValue() {

        var projectId = 1;
        var classId = 123212;
        var label = "test";
        var lang = "en";

        var k = ProjectClassLabelKey.newBuilder().setProjectId(projectId).setClassId(classId).build();
        var v = ProjectClassLabelValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setLabel(label)
                .setLanguageIso(lang)
                .setLanguageId(I.EN.get())
                .setDeleted$1(false)
                .build();
        projectClassLabel.pipeInput(k, v);

        var outRecords = outputTopic.readKeyValuesToMap();

        var expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/c123212> a <http://www.w3.org/2002/07/owl#Class> .")
                .build();

        var record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        var expectedKey2 = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/c123212> <http://www.w3.org/2000/01/rdf-schema#label> \"test\"@en .")
                .build();

        var record2 = outRecords.get(expectedKey2);
        assertThat(record2.getOperation()).isEqualTo(Operation.insert);

        var v2 = ProjectClassLabelValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setLabel(label)
                .setLanguageIso(lang)
                .setLanguageId(I.EN.get())
                .setDeleted$1(true)
                .build();
        projectClassLabel.pipeInput(k, v2);
        outRecords = outputTopic.readKeyValuesToMap();

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/c123212> a <http://www.w3.org/2002/07/owl#Class> .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/c123212> <http://www.w3.org/2000/01/rdf-schema#label> \"test\"@en .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);
    }

    /**
     * The goal of this test is to check if the key/value pairs generated in the processor match the expected list of key/value pairs
     */
    @Test
    void testListKeyValuePairs() {

        var projectId = 1;
        var classId = 123212;
        var label = "The \"foo\\bar\"";

        var k = ProjectClassLabelKey.newBuilder().setProjectId(projectId).setClassId(classId).build();
        var v = ProjectClassLabelValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setLabel(label)
                .setLanguageIso(null)
                .setLanguageId(null)
                .setDeleted$1(false)
                .build();
        projectClassLabel.pipeInput(k, v);

        List<KeyValue<ProjectRdfKey, ProjectRdfValue>> expectedList = new LinkedList<>();


        var outRecordsList = outputTopic.readKeyValuesToList();

        var expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/c123212> a <http://www.w3.org/2002/07/owl#Class> .")
                .build();
        var expectedValue = ProjectRdfValue.newBuilder()
                .setOperation(Operation.insert)
                .build();

        var expectedKey2 = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/c123212> <http://www.w3.org/2000/01/rdf-schema#label> \"The \\\"foo\\\\bar\\\"\"^^<http://www.w3.org/2001/XMLSchema#string> .")
                .build();
        var expectedValue2 = ProjectRdfValue.newBuilder()
                .setOperation(Operation.insert)
                .build();

        expectedList.add(KeyValue.pair(expectedKey, expectedValue));
        expectedList.add(KeyValue.pair(expectedKey2, expectedValue2));

        assertThat(expectedList.equals(outRecordsList)).isTrue();
    }
}
