package org.geovistory.toolbox.streams.analysis.statements.processors;


import io.debezium.data.geometry.Geography;
import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.analysis.statements.AnalysisConfluentAvroSerdes;
import org.geovistory.toolbox.streams.analysis.statements.Env;
import org.geovistory.toolbox.streams.analysis.statements.avro.AnalysisStatementValue;
import org.geovistory.toolbox.streams.analysis.statements.avro.ObjectInfoValue;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.GeoUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectAnalysisStatementTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectAnalysisStatementTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectStatementKey, ProjectStatementValue> projectStatementWithLiteralTopic;
    private TestInputTopic<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityTopic;
    private TestOutputTopic<AnalysisStatementKey, AnalysisStatementValue> outputTopic;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = ProjectAnalysisStatement.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        var localSerdes = new AnalysisConfluentAvroSerdes();

        projectStatementWithLiteralTopic = testDriver.createInputTopic(
                Env.INSTANCE.TOPIC_PROJECT_STATEMENT_WITH_LITERAL,
                avroSerdes.ProjectStatementKey().serializer(),
                avroSerdes.ProjectStatementValue().serializer());

        projectStatementWithEntityTopic = testDriver.createInputTopic(
                Env.INSTANCE.TOPIC_PROJECT_STATEMENT_WITH_ENTITY,
                avroSerdes.ProjectStatementKey().serializer(),
                avroSerdes.ProjectStatementValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                ProjectAnalysisStatement.output.TOPICS.project_analysis_statement,
                localSerdes.AnalysisStatementKey().deserializer(),
                localSerdes.AnalysisStatementValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }


    @Test
    void testAppellation() throws IOException {


        var projectId = 1;
        var statementId = 2;

        // add a class label
        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId("i8")
                                .setPropertyId(2)
                                .setObjectId("i9")
                                .setObject(NodeValue.newBuilder().setLabel("Name 2").setId("i2").setClassId(1)
                                        .setAppellation(
                                                Appellation.newBuilder()
                                                        .setFkClass(1)
                                                        .setString("Name 2")
                                                        .build())
                                        .build()).build()
                )
                .setOrdNumOfDomain(1)
                .setOrdNumOfRange(2)
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var expectedKey = AnalysisStatementKey.newBuilder().setProject(projectId).setPkEntity(statementId).build();
        var record = outRecords.get(expectedKey);
        var objectInfoValue = objectMapper.readValue(record.getObjectInfoValue(), ObjectInfoValue.class);

        assertThat(objectInfoValue.getString().getString()).isEqualTo("Name 2");

    }

    @Test
    void testTimePrimitive() throws IOException {


        var projectId = 1;
        var statementId = 2;

        // add a class label
        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId("i8")
                                .setPropertyId(2)
                                .setObjectId("i9")
                                .setObject(NodeValue.newBuilder().setLabel("Name 2").setId("i2").setClassId(1)
                                        .setTimePrimitive(
                                                TimePrimitive.newBuilder()
                                                        .setFkClass(0)
                                                        .setPkEntity(0)
                                                        .setJulianDay(2362729)
                                                        .setDuration("1 day")
                                                        .setCalendar("gregorian")
                                                        .build()
                                        ).build()).build()
                )
                .setOrdNumOfDomain(1)
                .setOrdNumOfRange(2)
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var expectedKey = AnalysisStatementKey.newBuilder().setProject(projectId).setPkEntity(statementId).build();
        var record = outRecords.get(expectedKey);
        var objectInfoValue = objectMapper.readValue(record.getObjectInfoValue(), ObjectInfoValue.class);

        assertThat(objectInfoValue.getTimePrimitive().getJulianDay()).isEqualTo(2362729);

    }


    @Test
    void testLanguage() throws IOException {


        var projectId = 1;
        var statementId = 2;

        // add a class label
        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId("i8")
                                .setPropertyId(2)
                                .setObjectId("i9")
                                .setObject(NodeValue.newBuilder().setLabel("Name 2").setId("i2").setClassId(1)
                                        .setLanguage(Language.newBuilder()
                                                .setFkClass(2)
                                                .setPkEntity(2)
                                                .setPkLanguage("1")
                                                .setNotes("Italian")
                                                .build()
                                        ).build()).build()
                )
                .setOrdNumOfDomain(1)
                .setOrdNumOfRange(2)
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var expectedKey = AnalysisStatementKey.newBuilder().setProject(projectId).setPkEntity(statementId).build();
        var record = outRecords.get(expectedKey);
        var objectInfoValue = objectMapper.readValue(record.getObjectInfoValue(), ObjectInfoValue.class);

        assertThat(objectInfoValue.getLanguage().getLabel()).isEqualTo("Italian");

    }


    @Test
    void testLangString() throws IOException {


        var projectId = 1;
        var statementId = 2;

        // add a class label
        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId("i8")
                                .setPropertyId(2)
                                .setObjectId("i9")
                                .setObject(NodeValue.newBuilder().setLabel("Name 2").setId("i2").setClassId(1)
                                        .setLangString(LangString.newBuilder()
                                                .setFkClass(2)
                                                .setPkEntity(2)
                                                .setString("Foo")
                                                .setFkLanguage(123)
                                                .build()
                                        ).build()).build()
                )
                .setOrdNumOfDomain(1)
                .setOrdNumOfRange(2)
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var expectedKey = AnalysisStatementKey.newBuilder().setProject(projectId).setPkEntity(statementId).build();
        var record = outRecords.get(expectedKey);
        var objectInfoValue = objectMapper.readValue(record.getObjectInfoValue(), ObjectInfoValue.class);

        assertThat(objectInfoValue.getLangString().getString()).isEqualTo("Foo");

    }


    @Test
    void testCell() throws IOException {


        var projectId = 1;
        var statementId = 2;

        // add a class label
        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId("i8")
                                .setPropertyId(2)
                                .setObjectId("i9")
                                .setObject(NodeValue.newBuilder().setLabel("Name 2").setId("i2").setClassId(1)
                                        .setCell(Cell.newBuilder()
                                                .setFkClass(2)
                                                .setNumericValue(2D)
                                                .setFkDigital(1)
                                                .setPkCell(1L)
                                                .setFkRow(2L)
                                                .setFkColumn(2)
                                                .build()
                                        ).build()).build()
                )
                .setOrdNumOfDomain(1)
                .setOrdNumOfRange(2)
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var expectedKey = AnalysisStatementKey.newBuilder().setProject(projectId).setPkEntity(statementId).build();
        var record = outRecords.get(expectedKey);
        var objectInfoValue = objectMapper.readValue(record.getObjectInfoValue(), ObjectInfoValue.class);

        assertThat(objectInfoValue.getCell().getFkColumn()).isEqualTo(2);

    }


    @Test
    void testDimension() throws IOException {


        var projectId = 1;
        var statementId = 2;

        // add a class label
        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId("i8")
                                .setPropertyId(2)
                                .setObjectId("i9")
                                .setObject(NodeValue.newBuilder().setLabel("Name 2").setId("i2").setClassId(1)
                                        .setDimension(Dimension.newBuilder()
                                                .setFkClass(2)
                                                .setNumericValue(2.3D)
                                                .setFkMeasurementUnit(2)
                                                .build()
                                        ).build()).build()
                )
                .setOrdNumOfDomain(1)
                .setOrdNumOfRange(2)
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var expectedKey = AnalysisStatementKey.newBuilder().setProject(projectId).setPkEntity(statementId).build();
        var record = outRecords.get(expectedKey);
        var objectInfoValue = objectMapper.readValue(record.getObjectInfoValue(), ObjectInfoValue.class);

        assertThat(objectInfoValue.getDimension().getNumericValue()).isEqualTo(2.3D);

    }


    @Test
    void testGeometry() throws IOException {


        var projectId = 1;
        var statementId = 2;

        // add a class label
        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId("i8")
                                .setPropertyId(2)
                                .setObjectId("i9")
                                .setObject(NodeValue.newBuilder().setLabel("Name 2").setId("i2").setClassId(1)
                                        .setPlace(Place.newBuilder()
                                                .setFkClass(2)
                                                .setGeoPoint(Geography.newBuilder()
                                                        .setWkb(GeoUtils.pointToBytes(33, 44, 4326))
                                                        .setSrid(4326)
                                                        .build())
                                                .build()
                                        ).build()).build()
                )
                .setOrdNumOfDomain(1)
                .setOrdNumOfRange(2)
                .setDeleted$1(false)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var expectedKey = AnalysisStatementKey.newBuilder().setProject(projectId).setPkEntity(statementId).build();
        var record = outRecords.get(expectedKey);
        var objectInfoValue = objectMapper.readValue(record.getObjectInfoValue(), ObjectInfoValue.class);

        assertThat(objectInfoValue.getGeometry().getGeoJSON().getCoordinates().get(0)).isEqualTo(33);

    }


}
