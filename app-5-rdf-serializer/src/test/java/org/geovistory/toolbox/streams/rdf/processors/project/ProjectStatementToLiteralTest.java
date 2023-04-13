package org.geovistory.toolbox.streams.rdf.processors.project;


import io.debezium.data.geometry.Geography;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.rdf.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectStatementToLiteralTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectStatementToLiteralTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
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
        var communityClassLabel = new ProjectStatementToLiteral(avroSerdes, registerInputTopic, outputTopicNames);
        communityClassLabel.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        projectStatementWithLiteralTopic = testDriver.createInputTopic(
                inputTopicNames. getProjectStatementWithLiteral(),
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
     * The goal of this test is to check if the output is not empty for the language
     */
    @Test
    void testLanguageOutputIsNotEmpty() {
        var projectId = 567;
        var statementId = 345;
        var subjectId = "i1761647";
        var objectId = "i2255949";
        var propertyId = 1112;
        var languageNotes = "italian";
        var pkLanguage = "it";

        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setLanguage(Language.newBuilder()
                                                .setNotes(languageNotes)
                                                .setPkLanguage(pkLanguage)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
    }

    /**
     * The goal of this test is to check if the output is not empty for the appellation
     */
    @Test
    void testAppellationOutputIsNotEmpty() {
        var projectId = 567;
        var statementId = 345;
        var subjectId = "i1761647";
        var objectId = "i2255949";
        var propertyId = 1112;
        var appellationString = "foo";

        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setAppellation(Appellation.newBuilder()
                                                .setFkClass(0)
                                                .setString(appellationString)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
    }

    /**
     * The goal of this test is to check if the output is not empty for the langString
     */
    @Test
    void testLangStringOutputIsNotEmpty() {
        var projectId = 567;
        var statementId = 345;
        var subjectId = "i1761647";
        var objectId = "i2255949";
        var propertyId = 1112;
        var langStringString = "Bar";
        var langStringFkLanguage = 19703;

        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setLangString(LangString.newBuilder()
                                                .setFkClass(0)
                                                .setString(langStringString)
                                                .setFkLanguage(langStringFkLanguage)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
    }

    /**
     * The goal of this test is to check if the output is not empty for the place
     */
    @Test
    void testPlaceOutputIsNotEmpty() {
        var projectId = 567;
        var statementId = 345;
        var subjectId = "i1761647";
        var objectId = "i2255949";
        var propertyId = 1112;
        var placeWkb = "AQEAACDmEAAA9DRgkPTJAkB9zAcEOm1IQA==";
        var placeSrid = 4326;


        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setPlace(Place.newBuilder()
                                                .setFkClass(2)
                                                .setGeoPoint(Geography.newBuilder()
                                                        .setWkb(ByteBuffer.wrap(placeWkb.getBytes(StandardCharsets.UTF_8)))
                                                        .setSrid(placeSrid)
                                                        .build())
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
    }

    /**
     * The goal of this test is to check if the output is not empty for the timePrimitive
     */
    @Test
    void testTimePrimitiveOutputIsNotEmpty() {
        var projectId = 567;
        var statementId = 345;
        var subjectId = "i1761647";
        var objectId = "i2255949";
        var propertyId = 71;
        var duration = "1 year";
        var julianDay = 2290483;
        var calendarType = "julian";


        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setTimePrimitive(TimePrimitive.newBuilder()
                                                .setFkClass(0)
                                                .setPkEntity(0)
                                                .setJulianDay(julianDay)
                                                .setDuration(duration)
                                                .setCalendar(calendarType)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
    }

    /**
     * The goal of this test is to check if the output is not empty for the dimension
     */
    @Test
    void testDimensionOutputIsNotEmpty() {
        var projectId = 567;
        var statementId = 345;
        var subjectId = "i1761647";
        var objectId = "i2255949";
        var propertyId = 40;
        var dimFkMeasurementUnit = 880899;
        var dimNumericValue = 10;

        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setDimension(Dimension.newBuilder()
                                                .setFkClass(0)
                                                .setPkEntity(0)
                                                .setNumericValue(dimNumericValue)
                                                .setFkMeasurementUnit(dimFkMeasurementUnit)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
    }

    /**
     * The goal of this test is to check if the language output has the correct size (should be equals to 1)
     */
    @Test
    void testSizeOfLanguageOutput() {
        var projectId = 567;
        var statementId = 345;
        var subjectId = "i1761647";
        var objectId = "i2255949";
        var propertyId = 1112;
        var languageNotes = "italian";
        var pkLanguage = "it";

        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setLanguage(Language.newBuilder()
                                                .setNotes(languageNotes)
                                                .setPkLanguage(pkLanguage)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);

        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
    }

    /**
     * The goal of this test is to check if the appellation output has the correct size (should be equals to 1)
     */
    @Test
    void testSizeOfAppellationOutput() {
        var projectId = 567;
        var statementId = 345;
        var subjectId = "i1761647";
        var objectId = "i2255949";
        var propertyId = 1112;
        var appellationString = "foo";


        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setAppellation(Appellation.newBuilder()
                                                .setFkClass(0)
                                                .setString(appellationString)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);

        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
    }

    /**
     * The goal of this test is to check if the langString output has the correct size (should be equals to 1)
     */
    @Test
    void testSizeOfLangStringOutput() {
        var projectId = 567;
        var statementId = 345;
        var subjectId = "i1761647";
        var objectId = "i2255949";
        var propertyId = 1112;
        var langStringString = "Bar";
        var langStringFkLanguage = 19703;

        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setLangString(LangString.newBuilder()
                                                .setFkClass(0)
                                                .setString(langStringString)
                                                .setFkLanguage(langStringFkLanguage)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);

        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
    }

    /**
     * The goal of this test is to check if the place output has the correct size (should be equals to 1)
     */
    @Test
    void testSizeOfPlaceOutput() {
        var projectId = 567;
        var statementId = 345;
        var subjectId = "i1761647";
        var objectId = "i2255949";
        var propertyId = 1112;
        var placeWkb = "AQEAACDmEAAA9DRgkPTJAkB9zAcEOm1IQA==";
        var placeSrid = 4326;

        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setPlace(Place.newBuilder()
                                                .setFkClass(2)
                                                .setGeoPoint(Geography.newBuilder()
                                                        .setWkb(ByteBuffer.wrap(placeWkb.getBytes(StandardCharsets.UTF_8)))
                                                        .setSrid(placeSrid)
                                                        .build())
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);

        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
    }

    /**
     * The goal of this test is to check if the timePrimitive output has the correct size (should be equals to 9)
     */
    @Test
    void testSizeOfTimePrimitiveOutput() {
        var projectId = 567;
        var statementId = 345;
        var subjectId = "i1761647";
        var objectId = "i2255949";
        var propertyId = 71;
        var duration = "1 year";
        var julianDay = 2290483;
        var calendarType = "julian";


        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setTimePrimitive(TimePrimitive.newBuilder()
                                                .setFkClass(0)
                                                .setPkEntity(0)
                                                .setJulianDay(julianDay)
                                                .setDuration(duration)
                                                .setCalendar(calendarType)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);

        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(9);
    }

    /**
     * The goal of this test is to check if the dimension output has the correct size (should be equals to 5)
     */
    @Test
    void testSizeOfDimensionOutput() {
        var projectId = 567;
        var statementId = 345;
        var subjectId = "i1761647";
        var objectId = "i2255949";
        var propertyId = 40;
        var dimFkMeasurementUnit = 880899;
        var dimNumericValue = 10;


        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setDimension(Dimension.newBuilder()
                                                .setFkClass(0)
                                                .setPkEntity(0)
                                                .setNumericValue(dimNumericValue)
                                                .setFkMeasurementUnit(dimFkMeasurementUnit)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);

        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(5);
    }

    /**
     * The goal of this test is to check if the operation value (insert or delete) is correctly set in the output topic
     */
    @Test
    void testLanguageOperationValue() {
        var projectId = 567;
        var statementId = 345;
        var subjectId = "i1761647";
        var objectId = "i2255949";
        var propertyId = 1112;
        var languageNotes = "Italian";
        var pkLanguage = "it";

        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setLanguage(Language.newBuilder()
                                                .setNotes(languageNotes)
                                                .setPkLanguage(pkLanguage)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);

        var outRecords = outputTopic.readKeyValuesToMap();

        var expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p1112> \"Italian\"^^<http://www.w3.org/2001/XMLSchema#string> .")
                .build();

        var record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        var v2 = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setLanguage(Language.newBuilder()
                                                .setNotes(languageNotes)
                                                .setPkLanguage(pkLanguage)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(true)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v2);
        outRecords = outputTopic.readKeyValuesToMap();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);
    }

    /**
     * The goal of this test is to check if the operation value (insert or delete) is correctly set in the output topic
     */
    @Test
    void testAppellationOperationValue() {
        var projectId = 567;
        var statementId = 345;
        var subjectId = "i1761647";
        var objectId = "i2255949";
        var propertyId = 1113;
        var appellationString = "Foo";

        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setAppellation(Appellation.newBuilder()
                                                .setFkClass(0)
                                                .setString(appellationString)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);

        var outRecords = outputTopic.readKeyValuesToMap();

        var expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p1113> \"Foo\"^^<http://www.w3.org/2001/XMLSchema#string> .")
                .build();

        var record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        var v2 = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setAppellation(Appellation.newBuilder()
                                                .setFkClass(0)
                                                .setString(appellationString)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(true)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v2);
        outRecords = outputTopic.readKeyValuesToMap();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);
    }

    /**
     * The goal of this test is to check if the operation value (insert or delete) is correctly set in the output topic
     */
    @Test
    void testLangStringOperationValue() {
        var projectId = 567;
        var statementId = 345;
        var subjectId = "i1761647";
        var objectId = "i2255949";
        var propertyId = 1113;
        var langStringString = "Bar";
        var langStringFkLanguage = 19703;

        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setLangString(LangString.newBuilder()
                                                .setFkClass(0)
                                                .setString(langStringString)
                                                .setFkLanguage(langStringFkLanguage)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);

        var outRecords = outputTopic.readKeyValuesToMap();

        var expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p1113> \"Bar\"@it .")
                .build();

        var record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        var v2 = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setLangString(LangString.newBuilder()
                                                .setFkClass(0)
                                                .setString(langStringString)
                                                .setFkLanguage(langStringFkLanguage)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(true)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v2);
        outRecords = outputTopic.readKeyValuesToMap();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);
    }

    /**
     * The goal of this test is to check if the operation value (insert or delete) is correctly set in the output topic
     */
    @Test
    void testPlaceOperationValue() {
        var projectId = 567;
        var statementId = 345;
        var subjectId = "i1761647";
        var objectId = "i2255949";
        var propertyId = 1113;
        var placeWkb = "AQEAACDmEAAA9DRgkPTJAkB9zAcEOm1IQA==";
        var placeSrid = 4326;

        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setPlace(Place.newBuilder()
                                                .setFkClass(2)
                                                .setGeoPoint(Geography.newBuilder()
                                                        .setWkb(ByteBuffer.wrap(placeWkb.getBytes(StandardCharsets.UTF_8)))
                                                        .setSrid(placeSrid)
                                                        .build())
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);

        var outRecords = outputTopic.readKeyValuesToMap();

        var expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p1113> \"<http://www.opengis.net/def/crs/EPSG/0/4326>POINT(2.348611 48.853333)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .")
                .build();

        var record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        var v2 = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setPlace(Place.newBuilder()
                                                .setFkClass(2)
                                                .setGeoPoint(Geography.newBuilder()
                                                        .setWkb(ByteBuffer.wrap(placeWkb.getBytes(StandardCharsets.UTF_8)))
                                                        .setSrid(placeSrid)
                                                        .build())
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(true)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v2);
        outRecords = outputTopic.readKeyValuesToMap();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);
    }

    /**
     * The goal of this test is to check if the operation value (insert or delete) is correctly set in the output topic
     */
    @Test
    void testTimePrimitiveOperationValue() {
        var projectId = 567;
        var statementId = 345;
        var subjectId = "i1761647";
        var objectId = "i2255949";
        var propertyId = 71;
        var duration = "1 year";
        var julianDay = 2290483;
        var calendarType = "julian";

        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setTimePrimitive(TimePrimitive.newBuilder()
                                                .setFkClass(0)
                                                .setPkEntity(0)
                                                .setJulianDay(julianDay)
                                                .setDuration(duration)
                                                .setCalendar(calendarType)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);

        var outRecords = outputTopic.readKeyValuesToMap();

        var expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i1761647ts> <https://ontome.net/ontology/p71> <http://geovistory.org/resource/i2255949>")
                .build();

        var record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <https://ontome.net/ontology/p71i> <http://geovistory.org/resource/i1761647ts>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> a <http://www.w3.org/2006/time#DateTimeDescription>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <http://www.w3.org/2006/time#hasTRS> <https://d-nb.info/gnd/4318310-4>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <http://www.w3.org/2006/time#day> \"---01\"^^<http://www.w3.org/2006/time#generalDay>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <http://www.w3.org/2006/time#month> \"--01\"^^<http://www.w3.org/2006/time#generalMonth>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <http://www.w3.org/2006/time#year> \"-1559\"^^<http://www.w3.org/2006/time#generalYear>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <http://www.w3.org/2006/time#hasTRS> <http://www.w3.org/2006/time#unitYear>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <http://www.w3.org/2006/time#year> \"1559\"^^<http://www.w3.org/2001/XMLSchema#string>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        var v2 = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setTimePrimitive(TimePrimitive.newBuilder()
                                                .setFkClass(0)
                                                .setPkEntity(0)
                                                .setJulianDay(julianDay)
                                                .setDuration(duration)
                                                .setCalendar(calendarType)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(true)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v2);
        outRecords = outputTopic.readKeyValuesToMap();

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i1761647ts> <https://ontome.net/ontology/p71> <http://geovistory.org/resource/i2255949>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <https://ontome.net/ontology/p71i> <http://geovistory.org/resource/i1761647ts>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> a <http://www.w3.org/2006/time#DateTimeDescription>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <http://www.w3.org/2006/time#hasTRS> <https://d-nb.info/gnd/4318310-4>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <http://www.w3.org/2006/time#day> \"---01\"^^<http://www.w3.org/2006/time#generalDay>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <http://www.w3.org/2006/time#month> \"--01\"^^<http://www.w3.org/2006/time#generalMonth>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <http://www.w3.org/2006/time#year> \"-1559\"^^<http://www.w3.org/2006/time#generalYear>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <http://www.w3.org/2006/time#hasTRS> <http://www.w3.org/2006/time#unitYear>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <http://www.w3.org/2006/time#year> \"1559\"^^<http://www.w3.org/2001/XMLSchema#string>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);
    }

    /**
     * The goal of this test is to check if the operation value (insert or delete) is correctly set in the output topic
     */
    @Test
    void testDimensionOperationValue() {
        var projectId = 567;
        var statementId = 345;
        var subjectId = "i1761647";
        var objectId = "i2255949";
        var propertyId = 40;
        var dimFkMeasurementUnit = 880899;
        var dimNumericValue = 10;

        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setDimension(Dimension.newBuilder()
                                                .setFkClass(0)
                                                .setPkEntity(0)
                                                .setNumericValue(dimNumericValue)
                                                .setFkMeasurementUnit(dimFkMeasurementUnit)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);

        var outRecords = outputTopic.readKeyValuesToMap();

        var expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p40> <http://geovistory.org/resource/i2255949>")
                .build();

        var record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <https://ontome.net/ontology/p40i> <http://geovistory.org/resource/i1761647>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <https://ontome.net/ontology/p78> \"10\"^^<http://www.w3.org/2001/XMLSchema#decimal>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <https://ontome.net/ontology/p79> <http://geovistory.org/resource/i880899>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i880899> <https://ontome.net/ontology/p79i> <http://geovistory.org/resource/i2255949>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        var v2 = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .setObject(NodeValue.newBuilder()
                                        .setClassId(0)
                                        .setDimension(Dimension.newBuilder()
                                                .setFkClass(0)
                                                .setPkEntity(0)
                                                .setNumericValue(dimNumericValue)
                                                .setFkMeasurementUnit(dimFkMeasurementUnit)
                                                .build()
                                        ).build()
                                ).build()
                )
                .setDeleted$1(true)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v2);
        outRecords = outputTopic.readKeyValuesToMap();

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <https://ontome.net/ontology/p40i> <http://geovistory.org/resource/i1761647>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <https://ontome.net/ontology/p40i> <http://geovistory.org/resource/i1761647>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <https://ontome.net/ontology/p78> \"10\"^^<http://www.w3.org/2001/XMLSchema#decimal>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i2255949> <https://ontome.net/ontology/p79> <http://geovistory.org/resource/i880899>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://geovistory.org/resource/i880899> <https://ontome.net/ontology/p79i> <http://geovistory.org/resource/i2255949>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.delete);

    }
}
