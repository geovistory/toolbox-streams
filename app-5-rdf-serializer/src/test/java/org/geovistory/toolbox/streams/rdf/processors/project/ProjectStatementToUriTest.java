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

class ProjectStatementToUriTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectStatementToUriTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityTopic;
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
        var communityClassLabel = new ProjectStatementToUri(avroSerdes, registerInputTopic, outputTopicNames);
        communityClassLabel.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        projectStatementWithEntityTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectStatementWithEntity(),
                avroSerdes.ProjectStatementKey().serializer(),
                avroSerdes.ProjectStatementValue().serializer());

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
        var statementId = 123212;
        var subjectId = "i1";
        var objectId = "i2";
        var propertyId = 9;
        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithEntityTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
    }

    /**
     * The goal of this test is to check if the output has the correct size (should be equals to twice the number of statements in the input topic)
     */
    @Test
    void testSizeOfOutput() {

        var projectId = 1;
        var statementId = 123212;
        var subjectId = "i1";
        var objectId = "i2";
        var propertyId = 9;
        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithEntityTopic.pipeInput(k, v);

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
        var statementId = 123212;
        var subjectId = "i1";
        var objectId = "i2";
        var propertyId = 9;
        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithEntityTopic.pipeInput(k, v);

        var outRecords = outputTopic.readKeyValuesToMap();

        var expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i1> <https://ontome.net/ontology/p9> <http://geovistory.org/resource/i2>")
                .build();

        var record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        var expectedKey2 = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2> <https://ontome.net/ontology/p9i> <http://geovistory.org/resource/i1>")
                .build();

        var record2 = outRecords.get(expectedKey2);
        assertThat(record2.getOperation()).isEqualTo(Operation.insert);

        var v2 = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .build())
                .setDeleted$1(true)
                .build();
        projectStatementWithEntityTopic.pipeInput(k, v2);
        outRecords = outputTopic.readKeyValuesToMap();

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i1> <https://ontome.net/ontology/p9> <http://geovistory.org/resource/i2>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2> <https://ontome.net/ontology/p9i> <http://geovistory.org/resource/i1>")
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
        var statementId = 123212;
        var subjectId = "i1";
        var objectId = "i2";
        var propertyId = 9;
        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithEntityTopic.pipeInput(k, v);

        List<KeyValue<ProjectRdfKey, ProjectRdfValue>> expectedList = new LinkedList<>();


        var outRecordsList = outputTopic.readKeyValuesToList();

        var expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i1> <https://ontome.net/ontology/p9> <http://geovistory.org/resource/i2>")
                .build();
        var expectedValue = ProjectRdfValue.newBuilder()
                .setOperation(Operation.insert)
                .build();

        var expectedKey2 = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2> <https://ontome.net/ontology/p9i> <http://geovistory.org/resource/i1>")
                .build();
        var expectedValue2 = ProjectRdfValue.newBuilder()
                .setOperation(Operation.insert)
                .build();

        expectedList.add(KeyValue.pair(expectedKey, expectedValue));
        expectedList.add(KeyValue.pair(expectedKey2, expectedValue2));

        assertThat(expectedList.equals(outRecordsList)).isTrue();
    }
}
