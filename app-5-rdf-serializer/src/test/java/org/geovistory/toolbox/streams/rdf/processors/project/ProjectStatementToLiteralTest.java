package org.geovistory.toolbox.streams.rdf.processors.project;


import io.debezium.data.geometry.Geography;
import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.GeoUtils;
import org.geovistory.toolbox.streams.rdf.Env;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
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
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = ProjectStatementToLiteral.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        projectStatementWithLiteralTopic = testDriver.createInputTopic(
                Env.INSTANCE.TOPIC_PROJECT_STATEMENT_WITH_LITERAL,
                avroSerdes.ProjectStatementKey().serializer(),
                avroSerdes.ProjectStatementValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                ProjectStatementToUri.output.TOPICS.project_rdf,
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
        var appellationString = "foo";
        var langStringString = "Bar";
        var langStringFkLanguage = 19703;
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
        var languageNotes = "italian";
        var pkLanguage = "it";
        var appellationString = "foo";
        var langStringString = "Bar";
        var langStringFkLanguage = 19703;
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
        var languageNotes = "italian";
        var pkLanguage = "it";
        var appellationString = "foo";
        var langStringString = "Bar";
        var langStringFkLanguage = 19703;
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
    void testPlaceOutputIsNotEmpty() throws UnsupportedEncodingException {

        var projectId = 567;
        var statementId = 345;
        var subjectId = "i1761647";
        var objectId = "i2255949";
        var propertyId = 1112;
        var languageNotes = "italian";
        var pkLanguage = "it";
        var appellationString = "foo";
        var langStringString = "Bar";
        var langStringFkLanguage = 19703;
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
                                                        .setWkb(ByteBuffer.wrap(placeWkb.getBytes("UTF-8")))
                                                        .setSrid(4326)
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

}
