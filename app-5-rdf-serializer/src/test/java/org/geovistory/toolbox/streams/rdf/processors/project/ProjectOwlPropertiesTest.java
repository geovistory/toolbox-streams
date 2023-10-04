package org.geovistory.toolbox.streams.rdf.processors.project;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.rdf.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectOwlPropertiesTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectOwlPropertiesTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityTopic;
    private TestInputTopic<ProjectStatementKey, ProjectStatementValue> projectStatementWithLiteralTopic;
    private TestInputTopic<OntomePropertyLabelKey, OntomePropertyLabelValue> ontomePropertyLabelTopic;
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
        var projectOwlProperties = new ProjectOwlProperties(avroSerdes, registerInputTopic, outputTopicNames);
        projectOwlProperties.addProcessorsStandalone();
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

        ontomePropertyLabelTopic = testDriver.createInputTopic(
                inputTopicNames.getOntomePropertyLabel(),
                avroSerdes.OntomePropertyLabelKey().serializer(),
                avroSerdes.OntomePropertyLabelValue().serializer());

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
        var propertyId = 123;
        var subjectId = "i1";
        var statementId = 111;
        var objectId = "i200";
        var label = "test";
        var lang = I.EN.get();

        var kOntPrLab = OntomePropertyLabelKey.newBuilder().setPropertyId(propertyId).setLanguageId(lang).build();
        var vOntPrLab = OntomePropertyLabelValue.newBuilder().setPropertyId(propertyId).setLabel(label).setLanguageId(lang).build();
        ontomePropertyLabelTopic.pipeInput(kOntPrLab, vOntPrLab);

        var kStLit = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var vStLit = ProjectStatementValue.newBuilder()
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
        projectStatementWithLiteralTopic.pipeInput(kStLit, vStLit);

        var kStEnt = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var vStEnt = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .build()
                                )
                                .setPropertyId(propertyId)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithEntityTopic.pipeInput(kStEnt, vStEnt);

        assertThat(outputTopic.isEmpty()).isFalse();
    }

    /**
     * The goal of this test is to check if the output has the correct size: 13
     */
    @Test
    void testSizeOfOutput() {

        var projectId = 7;
        var propertyIdL = 123;
        var propertyIdL2 = 1843;
        var propertyIdE = 86;
        var propertyIdE2 = 1599;

        var propertyIdO = 86;
        var label = "brought into life";
        var inverseLabel = "was born";

        var propertyIdO2 = 1599;
        var label2 = "took place at";
        //var inverseLabel2 = null; not used

        var propertyIdO3 = 1843;
        var label3 = "has value";
        var inverseLabel3 = "is value of";

        var subjectId = "i1";
        var statementId = 111;
        var objectId = "i200";

        var lang = I.EN.get();

        var kOntPrLab = OntomePropertyLabelKey.newBuilder().setPropertyId(propertyIdO).setLanguageId(lang).build();
        var vOntPrLab = OntomePropertyLabelValue.newBuilder().setPropertyId(propertyIdO).setLabel(label).setLanguageId(lang).setInverseLabel(inverseLabel).build();
        ontomePropertyLabelTopic.pipeInput(kOntPrLab, vOntPrLab);

        var kOntPrLab2 = OntomePropertyLabelKey.newBuilder().setPropertyId(propertyIdO2).setLanguageId(lang).build();
        var vOntPrLab2 = OntomePropertyLabelValue.newBuilder().setPropertyId(propertyIdO2).setLabel(label2).setLanguageId(lang).setInverseLabel(null).build();
        ontomePropertyLabelTopic.pipeInput(kOntPrLab2, vOntPrLab2);

        var kOntPrLab3 = OntomePropertyLabelKey.newBuilder().setPropertyId(propertyIdO3).setLanguageId(lang).build();
        var vOntPrLab3 = OntomePropertyLabelValue.newBuilder().setPropertyId(propertyIdO3).setLabel(label3).setLanguageId(lang).setInverseLabel(inverseLabel3).build();
        ontomePropertyLabelTopic.pipeInput(kOntPrLab3, vOntPrLab3);

        var kStLit = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var vStLit = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyIdL)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(kStLit, vStLit);

        var kStLit2 = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var vStLit2 = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyIdL2)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(kStLit2, vStLit2);

        var kStEnt = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var vStEnt = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .build()
                                )
                                .setPropertyId(propertyIdE)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithEntityTopic.pipeInput(kStEnt, vStEnt);

        var kStEnt2 = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var vStEnt2 = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .build()
                                )
                                .setPropertyId(propertyIdE2)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithEntityTopic.pipeInput(kStEnt2, vStEnt2);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(13);
    }

    /**
     * The goal of this test is to check if the keys generated in the processor match the expected list of keys
     * The expectation is that the test turtle values are contained in the produced output topic
     */
    @Test
    void testListKeyValuePairs() {

        var projectId = 7;
        var propertyIdL = 123;
        var propertyIdL2 = 1843;
        var propertyIdE = 86;
        var propertyIdE2 = 1599;

        var propertyIdO = 86;
        var label = "brought into life";
        var inverseLabel = "was born";

        var propertyIdO2 = 1599;
        var label2 = "took place at";
        //var inverseLabel2 = null; not used

        var propertyIdO3 = 1843;
        var label3 = "has value";
        var inverseLabel3 = "is value of";

        var subjectId = "i1";
        var statementId = 111;
        var objectId = "i200";

        var lang = I.EN.get();

        var kOntPrLab = OntomePropertyLabelKey.newBuilder().setPropertyId(propertyIdO).setLanguageId(lang).build();
        var vOntPrLab = OntomePropertyLabelValue.newBuilder().setPropertyId(propertyIdO).setLabel(label).setLanguageId(lang).setInverseLabel(inverseLabel).build();
        ontomePropertyLabelTopic.pipeInput(kOntPrLab, vOntPrLab);

        var kOntPrLab2 = OntomePropertyLabelKey.newBuilder().setPropertyId(propertyIdO2).setLanguageId(lang).build();
        var vOntPrLab2 = OntomePropertyLabelValue.newBuilder().setPropertyId(propertyIdO2).setLabel(label2).setLanguageId(lang).setInverseLabel(null).build();
        ontomePropertyLabelTopic.pipeInput(kOntPrLab2, vOntPrLab2);

        var kOntPrLab3 = OntomePropertyLabelKey.newBuilder().setPropertyId(propertyIdO3).setLanguageId(lang).build();
        var vOntPrLab3 = OntomePropertyLabelValue.newBuilder().setPropertyId(propertyIdO3).setLabel(label3).setLanguageId(lang).setInverseLabel(inverseLabel3).build();
        ontomePropertyLabelTopic.pipeInput(kOntPrLab3, vOntPrLab3);

        var kStLit = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var vStLit = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyIdL)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(kStLit, vStLit);

        var kStLit2 = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var vStLit2 = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyIdL2)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(kStLit2, vStLit2);

        var kStEnt = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var vStEnt = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .build()
                                )
                                .setPropertyId(propertyIdE)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithEntityTopic.pipeInput(kStEnt, vStEnt);

        var kStEnt2 = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var vStEnt2 = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .build()
                                )
                                .setPropertyId(propertyIdE2)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithEntityTopic.pipeInput(kStEnt2, vStEnt2);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();


        var expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/p123> a <http://www.w3.org/2002/07/owl#DatatypeProperty> .")
                .build();

        var expectedKey2 = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/p1843> a <http://www.w3.org/2002/07/owl#DatatypeProperty> .")
                .build();
        var expectedKey3 = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/p1843> <http://www.w3.org/2000/01/rdf-schema#label> \"has value\"@en .")
                .build();
        var expectedKey4 = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/p1843i> <http://www.w3.org/2000/01/rdf-schema#label> \"is value of\"@en .")
                .build();
        var expectedKey5 = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/p86> a <http://www.w3.org/2002/07/owl#ObjectProperty> .")
                .build();
        var expectedKey6 = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/p86i> a <http://www.w3.org/2002/07/owl#ObjectProperty> .")
                .build();
        var expectedKey7 = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/p86i> <http://www.w3.org/2002/07/owl#inverseOf> <https://ontome.net/ontology/p86> .")
                .build();
        var expectedKey8 = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/p86> <http://www.w3.org/2000/01/rdf-schema#label> \"brought into life\"@en .")
                .build();
        var expectedKey9 = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/p86i> <http://www.w3.org/2000/01/rdf-schema#label> \"was born\"@en .")
                .build();
        var expectedKey10 = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/p1599> a <http://www.w3.org/2002/07/owl#ObjectProperty> .")
                .build();
        var expectedKey11 = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/p1599i> a <http://www.w3.org/2002/07/owl#ObjectProperty> .")
                .build();
        var expectedKey12 = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/p1599i> <http://www.w3.org/2002/07/owl#inverseOf> <https://ontome.net/ontology/p1599> .")
                .build();
        var expectedKey13 = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/p1599> <http://www.w3.org/2000/01/rdf-schema#label> \"took place at\"@en .")
                .build();

        assertThat(outRecords.containsKey(expectedKey)).isTrue();
        assertThat(outRecords.containsKey(expectedKey2)).isTrue();
        assertThat(outRecords.containsKey(expectedKey3)).isTrue();
        assertThat(outRecords.containsKey(expectedKey4)).isTrue();
        assertThat(outRecords.containsKey(expectedKey5)).isTrue();
        assertThat(outRecords.containsKey(expectedKey6)).isTrue();
        assertThat(outRecords.containsKey(expectedKey7)).isTrue();
        assertThat(outRecords.containsKey(expectedKey8)).isTrue();
        assertThat(outRecords.containsKey(expectedKey9)).isTrue();
        assertThat(outRecords.containsKey(expectedKey10)).isTrue();
        assertThat(outRecords.containsKey(expectedKey11)).isTrue();
        assertThat(outRecords.containsKey(expectedKey12)).isTrue();
        assertThat(outRecords.containsKey(expectedKey13)).isTrue();
    }

}