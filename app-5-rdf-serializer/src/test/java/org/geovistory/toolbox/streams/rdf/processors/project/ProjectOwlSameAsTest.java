package org.geovistory.toolbox.streams.rdf.processors.project;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.rdf.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectOwlSameAsTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectOwlSameAsTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityTopic;
    private TestInputTopic<ProjectStatementKey, ProjectStatementValue> projectStatementWithLiteralTopic;
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
        var projectOwlSameAs = new ProjectOwlSameAs(avroSerdes, registerInputTopic, outputTopicNames);
        projectOwlSameAs.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        projectStatementWithEntityTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectStatementWithEntity(),
                avroSerdes.ProjectStatementKey().serializer(),
                avroSerdes.ProjectStatementValue().serializer());

        projectStatementWithLiteralTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectStatementWithLiteral(),
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

        var projectIdLeft = 1;
        var subjectIdLeft = "i1";
        var statementIdLeft = 111;
        var objectIdLeft = "i200";
        var propertyIdLeft = 1943;

        var projectIdRight = 1;
        var subjectIdRight = "i200";
        var statementIdRight = 999;
        var objectAppellationStringRight = "https://wikidata/i345";
        var propertyIdRight = 1843;

        var kLeft = ProjectStatementKey.newBuilder().setProjectId(projectIdLeft).setStatementId(statementIdLeft).build();
        var vLeft = ProjectStatementValue.newBuilder()
                .setProjectId(projectIdLeft)
                .setStatementId(statementIdLeft)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectIdLeft)
                                .setObjectId(objectIdLeft)
                                .setPropertyId(propertyIdLeft)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithEntityTopic.pipeInput(kLeft, vLeft);

        var kRight = ProjectStatementKey.newBuilder().setProjectId(projectIdRight).setStatementId(statementIdRight).build();
        var vRight = ProjectStatementValue.newBuilder()
                .setProjectId(projectIdRight)
                .setStatementId(statementIdRight)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectIdRight)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setAppellation(Appellation.newBuilder()
                                                .setFkClass(0)
                                                .setString(objectAppellationStringRight)
                                                .build()
                                        ).build()
                                )
                                .setPropertyId(propertyIdRight)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(kRight, vRight);

        assertThat(outputTopic.isEmpty()).isFalse();
    }

    /**
     * The goal of this test is to check if the output has the correct size: 1 (one record for one sameAs)
     */
    @Test
    void testSizeOfOutput() {

        var projectIdLeft = 1;
        var subjectIdLeft = "i1";
        var statementIdLeft = 111;
        var objectIdLeft = "i200";
        var propertyIdLeft = 1943;

        var projectIdRight = 1;
        var subjectIdRight = "i200";
        var statementIdRight = 999;
        var objectAppellationStringRight = "https://wikidata/i345";
        var propertyIdRight = 1843;

        var kLeft = ProjectStatementKey.newBuilder().setProjectId(projectIdLeft).setStatementId(statementIdLeft).build();
        var vLeft = ProjectStatementValue.newBuilder()
                .setProjectId(projectIdLeft)
                .setStatementId(statementIdLeft)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectIdLeft)
                                .setObjectId(objectIdLeft)
                                .setPropertyId(propertyIdLeft)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithEntityTopic.pipeInput(kLeft, vLeft);

        var kRight = ProjectStatementKey.newBuilder().setProjectId(projectIdRight).setStatementId(statementIdRight).build();
        var vRight = ProjectStatementValue.newBuilder()
                .setProjectId(projectIdRight)
                .setStatementId(statementIdRight)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectIdRight)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setAppellation(Appellation.newBuilder()
                                                .setFkClass(0)
                                                .setString(objectAppellationStringRight)
                                                .build()
                                        ).build()
                                )
                                .setPropertyId(propertyIdRight)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(kRight, vRight);

        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
    }

    /**
     * The goal of this test is to check if the operation value "delete" is correctly set in the output topic
     */
    @Test
    void testOperationValueDelete() {

        var projectIdLeft = 1;
        var subjectIdLeft = "i1";
        var statementIdLeft = 111;
        var objectIdLeft = "i200";
        var propertyIdLeft = 1943;

        var projectIdRight = 1;
        var subjectIdRight = "i200";
        var statementIdRight = 999;
        var objectAppellationStringRight = "https://wikidata/i345";
        var propertyIdRight = 1843;

        var kLeft = ProjectStatementKey.newBuilder().setProjectId(projectIdLeft).setStatementId(statementIdLeft).build();
        var vLeft = ProjectStatementValue.newBuilder()
                .setProjectId(projectIdLeft)
                .setStatementId(statementIdLeft)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectIdLeft)
                                .setObjectId(objectIdLeft)
                                .setPropertyId(propertyIdLeft)
                                .build())
                .setDeleted$1(true)
                .build();
        projectStatementWithEntityTopic.pipeInput(kLeft, vLeft);

        var kRight = ProjectStatementKey.newBuilder().setProjectId(projectIdRight).setStatementId(statementIdRight).build();
        var vRight = ProjectStatementValue.newBuilder()
                .setProjectId(projectIdRight)
                .setStatementId(statementIdRight)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectIdRight)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setAppellation(Appellation.newBuilder()
                                                .setFkClass(0)
                                                .setString(objectAppellationStringRight)
                                                .build()
                                        ).build()
                                )
                                .setPropertyId(propertyIdRight)
                                .build())
                .setDeleted$1(true)
                .build();
        projectStatementWithLiteralTopic.pipeInput(kRight, vRight);

        var outRecords = outputTopic.readKeyValuesToMap();

        var expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectIdLeft)
                .setTurtle("<http://geovistory.org/resource/" + subjectIdLeft + "> <http://www.w3.org/2002/07/owl#sameAs> <" + objectAppellationStringRight + "> .")
                .build();

        var record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);
    }

    /**
     * The goal of this test is to check if the operation value "insert" is correctly set in the output topic
     */
    @Test
    void testOperationValueInsert() {

        var projectIdLeft = 1;
        var subjectIdLeft = "i1";
        var statementIdLeft = 111;
        var objectIdLeft = "i200";
        var propertyIdLeft = 1943;

        var projectIdRight = 1;
        var subjectIdRight = "i200";
        var statementIdRight = 999;
        var objectAppellationStringRight = "https://wikidata/i345";
        var propertyIdRight = 1843;

        var kLeft = ProjectStatementKey.newBuilder().setProjectId(projectIdLeft).setStatementId(statementIdLeft).build();
        var vLeft = ProjectStatementValue.newBuilder()
                .setProjectId(projectIdLeft)
                .setStatementId(statementIdLeft)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectIdLeft)
                                .setObjectId(objectIdLeft)
                                .setPropertyId(propertyIdLeft)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithEntityTopic.pipeInput(kLeft, vLeft);

        var kRight = ProjectStatementKey.newBuilder().setProjectId(projectIdRight).setStatementId(statementIdRight).build();
        var vRight = ProjectStatementValue.newBuilder()
                .setProjectId(projectIdRight)
                .setStatementId(statementIdRight)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectIdRight)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setAppellation(Appellation.newBuilder()
                                                .setFkClass(0)
                                                .setString(objectAppellationStringRight)
                                                .build()
                                        ).build()
                                )
                                .setPropertyId(propertyIdRight)
                                .build())
                .setDeleted$1(true)
                .build();
        projectStatementWithLiteralTopic.pipeInput(kRight, vRight);

        var outRecords = outputTopic.readKeyValuesToMap();

        var expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectIdLeft)
                .setTurtle("<http://geovistory.org/resource/" + subjectIdLeft + "> <http://www.w3.org/2002/07/owl#sameAs> <" + objectAppellationStringRight + "> .")
                .build();

        var record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);
    }

    /**
     * The goal of this test is to check if the URI used for the sameAs relation is correctly validated by the regex
     * In this case, a malformed URI is used, so we expect the output topic to be empty
     */
    @Test
    void testWrongUriRegexValue() {

        var projectIdLeft = 1;
        var subjectIdLeft = "i1";
        var statementIdLeft = 111;
        var objectIdLeft = "i200";
        var propertyIdLeft = 1943;

        var projectIdRight = 1;
        var subjectIdRight = "i200";
        var statementIdRight = 999;
        var objectAppellationStringRight = "wikidata/i345"; //wrong URI used to test the regex
        var propertyIdRight = 1843;

        var kLeft = ProjectStatementKey.newBuilder().setProjectId(projectIdLeft).setStatementId(statementIdLeft).build();
        var vLeft = ProjectStatementValue.newBuilder()
                .setProjectId(projectIdLeft)
                .setStatementId(statementIdLeft)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectIdLeft)
                                .setObjectId(objectIdLeft)
                                .setPropertyId(propertyIdLeft)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithEntityTopic.pipeInput(kLeft, vLeft);

        var kRight = ProjectStatementKey.newBuilder().setProjectId(projectIdRight).setStatementId(statementIdRight).build();
        var vRight = ProjectStatementValue.newBuilder()
                .setProjectId(projectIdRight)
                .setStatementId(statementIdRight)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectIdRight)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setAppellation(Appellation.newBuilder()
                                                .setFkClass(0)
                                                .setString(objectAppellationStringRight)
                                                .build()
                                        ).build()
                                )
                                .setPropertyId(propertyIdRight)
                                .build())
                .setDeleted$1(true)
                .build();
        projectStatementWithLiteralTopic.pipeInput(kRight, vRight);

        projectStatementWithLiteralTopic.pipeInput(kRight, vRight);

        assertThat(outputTopic.isEmpty()).isTrue();
    }
}
