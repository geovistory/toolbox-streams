package org.geovistory.toolbox.streams.analysis.statements.processors;


import io.debezium.data.geometry.Geography;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.analysis.statements.*;
import org.geovistory.toolbox.streams.analysis.statements.avro.AnalysisStatementKey;
import org.geovistory.toolbox.streams.analysis.statements.avro.AnalysisStatementValue;
import org.geovistory.toolbox.streams.analysis.statements.avro.ObjectInfoValue;
import org.geovistory.toolbox.streams.avro.*;
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

        var builderSingleton = new BuilderSingleton();
        var avroSerdes = new AvroSerdes();
        avroSerdes.QUARKUS_KAFKA_STREAMS_SCHEMA_REGISTRY_URL = MOCK_SCHEMA_REGISTRY_URL;
        var inputTopicNames = new InputTopicNames();
        var outputTopicNames = new OutputTopicNames();
        var registerInputTopic = new RegisterInputTopic(avroSerdes, builderSingleton, inputTopicNames);
        var projectAnalysisStatement = new ProjectAnalysisStatement(avroSerdes, registerInputTopic, outputTopicNames);
        projectAnalysisStatement.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        projectStatementWithLiteralTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectStatementWithLiteral(),
                avroSerdes.ProjectStatementKey().serializer(),
                avroSerdes.ProjectStatementValue().serializer());

        projectStatementWithEntityTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectStatementWithEntity(),
                avroSerdes.ProjectStatementKey().serializer(),
                avroSerdes.ProjectStatementValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectAnalysisStatement(),
                avroSerdes.AnalysisStatementKey().deserializer(),
                avroSerdes.AnalysisStatementValue().deserializer());
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
                                                        .setJulianDay(2290483)
                                                        .setDuration("1 year")
                                                        .setCalendar("julian")
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

        assertThat(objectInfoValue.getTimePrimitive().getJulianDay()).isEqualTo(2290483);

        assertThat(objectInfoValue.getTimePrimitive().getLabel()).isEqualTo("1559-01-01 (1 year)");

        assertThat(objectInfoValue.getTimePrimitive().getFrom().getCalJulian()).isEqualTo("1559-01-01");
        assertThat(objectInfoValue.getTimePrimitive().getFrom().getJulianDay()).isEqualTo(2290483);
        assertThat(objectInfoValue.getTimePrimitive().getFrom().getCalGregorian()).isEqualTo("1559-01-11");
        assertThat(objectInfoValue.getTimePrimitive().getFrom().getJulianSecond()).isEqualTo(197897731200L);
        assertThat(objectInfoValue.getTimePrimitive().getFrom().getCalGregorianIso8601()).isEqualTo("1559-01-11T00:00:00Z");

        assertThat(objectInfoValue.getTimePrimitive().getTo().getCalJulian()).isEqualTo("1560-01-01");
        assertThat(objectInfoValue.getTimePrimitive().getTo().getJulianDay()).isEqualTo(2290848);
        assertThat(objectInfoValue.getTimePrimitive().getTo().getCalGregorian()).isEqualTo("1560-01-11");
        assertThat(objectInfoValue.getTimePrimitive().getTo().getJulianSecond()).isEqualTo(197929267200L);
        assertThat(objectInfoValue.getTimePrimitive().getTo().getCalGregorianIso8601()).isEqualTo("1560-01-11T00:00:00Z");

      /*  assertThat(record.getObjectInfoValue().contains("cell")).isEqualTo(false);
        assertThat(record.getObjectInfoValue().contains("dimension")).isEqualTo(false);
        assertThat(record.getObjectInfoValue().contains("geometry")).isEqualTo(false);
        assertThat(record.getObjectInfoValue().contains("langString")).isEqualTo(false);
        assertThat(record.getObjectInfoValue().contains("string")).isEqualTo(false);*/

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
    void testLangStringWithNullString() throws IOException {
        var projectId = 1;
        var statementId = 2;

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
                                                .setString(null)
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

        assertThat(objectInfoValue.getLangString().getString()).isEqualTo("");
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
