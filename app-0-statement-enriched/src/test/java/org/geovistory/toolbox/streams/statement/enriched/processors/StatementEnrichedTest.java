package org.geovistory.toolbox.streams.statement.enriched.processors;


import io.debezium.data.geometry.Geography;
import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.GeoUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class StatementEnrichedTest {

    private static final String SCHEMA_REGISTRY_SCOPE = StatementEnrichedTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<dev.information.statement.Key, dev.information.statement.Value> infStatementTopic;
    private TestInputTopic<dev.information.resource.Key, dev.information.resource.Value> infResourceTopic;
    private TestInputTopic<dev.information.language.Key, dev.information.language.Value> infLanguageTopic;
    private TestInputTopic<dev.information.appellation.Key, dev.information.appellation.Value> infAppellationTopic;
    private TestInputTopic<dev.information.lang_string.Key, dev.information.lang_string.Value> infLangStringTopic;
    private TestInputTopic<dev.information.place.Key, dev.information.place.Value> infPlaceTopic;
    private TestInputTopic<dev.information.time_primitive.Key, dev.information.time_primitive.Value> infTimePrimitiveTopic;
    private TestInputTopic<dev.information.dimension.Key, dev.information.dimension.Value> infDimensionTopic;
    private TestInputTopic<dev.data.digital.Key, dev.data.digital.Value> datDigitalTopic;
    private TestInputTopic<dev.tables.cell.Key, dev.tables.cell.Value> tabCellTopic;
    private TestOutputTopic<dev.information.statement.Key, StatementEnrichedValue> statementWithEntityTopic;
    private TestOutputTopic<dev.information.statement.Key, StatementEnrichedValue> statementWithLiteralTopic;
    private TestOutputTopic<dev.information.statement.Key, StatementEnrichedValue> statementOtherTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = StatementEnriched.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        infStatementTopic = testDriver.createInputTopic(
                StatementEnriched.input.TOPICS.inf_statement,
                avroSerdes.InfStatementKey().serializer(),
                avroSerdes.InfStatementValue().serializer()
        );
        infResourceTopic = testDriver.createInputTopic(
                StatementEnriched.input.TOPICS.inf_resource,
                avroSerdes.InfResourceKey().serializer(),
                avroSerdes.InfResourceValue().serializer()
        );
        infLanguageTopic = testDriver.createInputTopic(
                StatementEnriched.input.TOPICS.inf_language,
                avroSerdes.InfLanguageKey().serializer(),
                avroSerdes.InfLanguageValue().serializer()
        );
        infAppellationTopic = testDriver.createInputTopic(
                StatementEnriched.input.TOPICS.inf_appellation,
                avroSerdes.InfAppellationKey().serializer(),
                avroSerdes.InfAppellationValue().serializer()
        );
        infLangStringTopic = testDriver.createInputTopic(
                StatementEnriched.input.TOPICS.inf_lang_string,
                avroSerdes.InfLangStringKey().serializer(),
                avroSerdes.InfLangStringValue().serializer()
        );
        infPlaceTopic = testDriver.createInputTopic(
                StatementEnriched.input.TOPICS.inf_place,
                avroSerdes.InfPlaceKey().serializer(),
                avroSerdes.InfPlaceValue().serializer()
        );
        infTimePrimitiveTopic = testDriver.createInputTopic(
                StatementEnriched.input.TOPICS.inf_time_primitive,
                avroSerdes.InfTimePrimitiveKey().serializer(),
                avroSerdes.InfTimePrimitiveValue().serializer()
        );
        infDimensionTopic = testDriver.createInputTopic(
                StatementEnriched.input.TOPICS.inf_dimension,
                avroSerdes.InfDimensionKey().serializer(),
                avroSerdes.InfDimensionValue().serializer()
        );
        tabCellTopic = testDriver.createInputTopic(
                StatementEnriched.input.TOPICS.tab_cell,
                avroSerdes.TabCellKey().serializer(),
                avroSerdes.TabCellValue().serializer()
        );
        datDigitalTopic = testDriver.createInputTopic(
                StatementEnriched.input.TOPICS.dat_digital,
                avroSerdes.DatDigitalKey().serializer(),
                avroSerdes.DatDigitalValue().serializer()
        );


        statementWithEntityTopic = testDriver.createOutputTopic(
                StatementEnriched.output.TOPICS.statement_with_entity,
                avroSerdes.InfStatementKey().deserializer(),
                avroSerdes.StatementEnrichedValue().deserializer());


        statementWithLiteralTopic = testDriver.createOutputTopic(
                StatementEnriched.output.TOPICS.statement_with_literal,
                avroSerdes.InfStatementKey().deserializer(),
                avroSerdes.StatementEnrichedValue().deserializer());

        statementOtherTopic = testDriver.createOutputTopic(
                StatementEnriched.output.TOPICS.statement_with_literal,
                avroSerdes.InfStatementKey().deserializer(),
                avroSerdes.StatementEnrichedValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testStatementWithoutObject() {
        int subjectId = 10;
        int propertyId = 20;
        int objectId = 30;

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = dev.information.statement.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setFkSubjectInfo(subjectId)
                .setFkProperty(propertyId)
                .setFkObjectInfo(objectId)
                .build();
        infStatementTopic.pipeInput(kS, vS);

        assertThat(statementWithEntityTopic.isEmpty()).isTrue();
        assertThat(statementWithLiteralTopic.isEmpty()).isTrue();
        assertThat(statementOtherTopic.isEmpty()).isTrue();

    }

    @Test
    void testStatementWithEntity() {
        int subjectId = 10;
        int propertyId = 20;
        int objectId = 30;
        int classId = 40;

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = dev.information.statement.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setFkSubjectInfo(subjectId)
                .setFkProperty(propertyId)
                .setFkObjectInfo(objectId)
                .build();
        infStatementTopic.pipeInput(kS, vS);

        // add subject entity
        var k2 = dev.information.resource.Key.newBuilder().setPkEntity(subjectId).build();
        var v2 = dev.information.resource.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(subjectId)
                .setFkClass(classId)
                .setCommunityVisibility("{ \"toolbox\": false, \"dataApi\": true, \"website\": false}")
                .build();
        infResourceTopic.pipeInput(k2, v2);

        // add object entity
        var k = dev.information.resource.Key.newBuilder().setPkEntity(objectId).build();
        var v = dev.information.resource.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(objectId)
                .setFkClass(classId)
                .setCommunityVisibility("{ \"toolbox\": true, \"dataApi\": false, \"website\": true}")
                .build();
        infResourceTopic.pipeInput(k, v);

        assertThat(statementWithEntityTopic.isEmpty()).isFalse();
        var outRecords = statementWithEntityTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kS);
        assertThat(record.getObjectClassId()).isEqualTo(classId);
        assertThat(record.getObject().getEntity().getCommunityVisibilityToolbox()).isEqualTo(true);
        assertThat(record.getObject().getEntity().getCommunityVisibilityDataApi()).isEqualTo(false);
        assertThat(record.getObject().getEntity().getCommunityVisibilityWebsite()).isEqualTo(true);

        assertThat(record.getSubject().getEntity().getCommunityVisibilityToolbox()).isEqualTo(false);
        assertThat(record.getSubject().getEntity().getCommunityVisibilityDataApi()).isEqualTo(true);
        assertThat(record.getSubject().getEntity().getCommunityVisibilityWebsite()).isEqualTo(false);
    }

    @Test
    void testStatementWithAppellation() {
        int objectId = 30;
        int classId = 40;
        int subjectId = 10;

        // add subject entity
        var k2 = dev.information.resource.Key.newBuilder().setPkEntity(subjectId).build();
        var v2 = dev.information.resource.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(subjectId)
                .setFkClass(classId)
                .setCommunityVisibility("{ \"toolbox\": false, \"dataApi\": true, \"website\": false}")
                .build();

        infResourceTopic.pipeInput(k2, v2);

        // string with more than 100
        String label = "Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_";

        // string with 100 characters
        String expected = "Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_";

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = dev.information.statement.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setFkSubjectInfo(subjectId)
                .setFkProperty(20)
                .setFkObjectInfo(objectId)
                .build();
        infStatementTopic.pipeInput(kS, vS);

        // add appellation
        var k = dev.information.appellation.Key.newBuilder().setPkEntity(objectId).build();
        var v = dev.information.appellation.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(0)
                .setQuillDoc("")
                .setPkEntity(objectId)
                .setFkClass(classId)
                .setString(label)
                .build();
        infAppellationTopic.pipeInput(k, v);

        assertThat(statementWithLiteralTopic.isEmpty()).isFalse();
        var outRecords = statementWithLiteralTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kS);
        assertThat(record.getObject().getLabel()).isEqualTo(expected);
        assertThat(record.getObject().getAppellation()).isNotNull();
        assertThat(record.getObjectClassId()).isEqualTo(classId);
    }

    @Test
    void testStatementWithLanguage() {
        int objectId = 30;
        String label = "English";
        int subjectId = 10;

        // add subject entity
        var k2 = dev.information.resource.Key.newBuilder().setPkEntity(subjectId).build();
        var v2 = dev.information.resource.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(subjectId)
                .setFkClass(1)
                .setCommunityVisibility("{ \"toolbox\": false, \"dataApi\": true, \"website\": false}")
                .build();
        infResourceTopic.pipeInput(k2, v2);

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = dev.information.statement.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setFkSubjectInfo(subjectId)
                .setFkProperty(20)
                .setFkObjectInfo(objectId)
                .build();
        infStatementTopic.pipeInput(kS, vS);

        // add language
        var k = dev.information.language.Key.newBuilder().setPkLanguage("en").build();
        var v = dev.information.language.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkLanguage("en")
                .setPkEntity(objectId)
                .setFkClass(0)
                .setNotes(label)
                .build();
        infLanguageTopic.pipeInput(k, v);

        assertThat(statementWithLiteralTopic.isEmpty()).isFalse();
        var outRecords = statementWithLiteralTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kS);
        assertThat(record.getObject().getLabel()).isEqualTo(label);
        assertThat(record.getObject().getLanguage()).isNotNull();
    }

    @Test
    void testStatementWithLangString() {
        int objectId = 30;
        String label = "Label";
        int subjectId = 10;

        // add subject entity
        var k2 = dev.information.resource.Key.newBuilder().setPkEntity(subjectId).build();
        var v2 = dev.information.resource.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(subjectId)
                .setFkClass(1)
                .setCommunityVisibility("{ \"toolbox\": false, \"dataApi\": true, \"website\": false}")
                .build();
        infResourceTopic.pipeInput(k2, v2);

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = dev.information.statement.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setFkSubjectInfo(subjectId)
                .setFkProperty(20)
                .setFkObjectInfo(objectId)
                .build();
        infStatementTopic.pipeInput(kS, vS);

        // add lang_string
        var k = dev.information.lang_string.Key.newBuilder().setPkEntity(objectId).build();
        var v = dev.information.lang_string.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(objectId)
                .setFkLanguage(0)
                .setFkClass(0)
                .setString(label)
                .build();
        infLangStringTopic.pipeInput(k, v);

        assertThat(statementWithLiteralTopic.isEmpty()).isFalse();
        var outRecords = statementWithLiteralTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kS);
        assertThat(record.getObject().getLabel()).isEqualTo(label);
        assertThat(record.getObject().getLangString()).isNotNull();
    }

    @Test
    void testStatementWithNullLangString() {
        int objectId = 30;
        String label = null;
        int subjectId = 10;

        // add subject entity
        var k2 = dev.information.resource.Key.newBuilder().setPkEntity(subjectId).build();
        var v2 = dev.information.resource.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(subjectId)
                .setFkClass(1)
                .setCommunityVisibility("{ \"toolbox\": false, \"dataApi\": true, \"website\": false}")
                .build();
        infResourceTopic.pipeInput(k2, v2);

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = dev.information.statement.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setFkSubjectInfo(subjectId)
                .setFkProperty(20)
                .setFkObjectInfo(objectId)
                .build();
        infStatementTopic.pipeInput(kS, vS);

        // add lang_string
        var k = dev.information.lang_string.Key.newBuilder().setPkEntity(objectId).build();
        var v = dev.information.lang_string.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(objectId)
                .setFkLanguage(0)
                .setFkClass(0)
                .setString(label)
                .build();
        infLangStringTopic.pipeInput(k, v);

        assertThat(statementWithLiteralTopic.isEmpty()).isFalse();
        var outRecords = statementWithLiteralTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kS);
        assertThat(record.getObject().getLabel()).isEqualTo(label);
        assertThat(record.getObject().getLangString()).isNotNull();
    }

    @Test
    void testStatementWithPlace() {
        int objectId = 30;
        int subjectId = 10;

        // add subject entity
        var k2 = dev.information.resource.Key.newBuilder().setPkEntity(subjectId).build();
        var v2 = dev.information.resource.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(subjectId)
                .setFkClass(1)
                .setCommunityVisibility("{ \"toolbox\": false, \"dataApi\": true, \"website\": false}")
                .build();
        infResourceTopic.pipeInput(k2, v2);

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = dev.information.statement.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setFkSubjectInfo(subjectId)
                .setFkProperty(20)
                .setFkObjectInfo(objectId)
                .build();
        infStatementTopic.pipeInput(kS, vS);

        double x = 1;
        double y = 2;

        // add place
        var k = dev.information.place.Key.newBuilder().setPkEntity(objectId).build();
        var v = dev.information.place.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(objectId)
                .setGeoPoint(Geography.newBuilder()
                        .setWkb(GeoUtils.pointToBytes(x, y, 4326))
                        .setSrid(4326)
                        .build())
                .setFkClass(0)
                .build();
        infPlaceTopic.pipeInput(k, v);

        assertThat(statementWithLiteralTopic.isEmpty()).isFalse();
        var outRecords = statementWithLiteralTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kS);
        assertThat(record.getObject().getLabel()).isEqualTo("WGS84: " + x + "°, " + y + "°");
        assertThat(record.getObject().getPlace()).isNotNull();
    }


    @Test
    void testStatementWithTimePrimitive() {
        int objectId = 30;
        int subjectId = 10;

        // add subject entity
        var k2 = dev.information.resource.Key.newBuilder().setPkEntity(subjectId).build();
        var v2 = dev.information.resource.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(subjectId)
                .setFkClass(1)
                .setCommunityVisibility("{ \"toolbox\": false, \"dataApi\": true, \"website\": false}")
                .build();
        infResourceTopic.pipeInput(k2, v2);

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = dev.information.statement.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setFkSubjectInfo(subjectId)
                .setFkProperty(20)
                .setFkObjectInfo(objectId)
                .build();
        infStatementTopic.pipeInput(kS, vS);


        // add place
        var k = dev.information.time_primitive.Key.newBuilder().setPkEntity(objectId).build();
        var v = dev.information.time_primitive.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(objectId)
                .setCalendar("gregorian")
                .setDuration("1 year")
                .setJulianDay(1234567)
                .setFkClass(0)
                .build();
        infTimePrimitiveTopic.pipeInput(k, v);

        assertThat(statementWithLiteralTopic.isEmpty()).isFalse();
        var outRecords = statementWithLiteralTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kS);
        assertThat(record.getObject().getLabel()).isNull();
        assertThat(record.getObject().getTimePrimitive()).isNotNull();
    }

    @Test
    void testStatementWithDimension() {
        int objectId = 30;
        double num = 111;
        int subjectId = 10;

        // add subject entity
        var k2 = dev.information.resource.Key.newBuilder().setPkEntity(subjectId).build();
        var v2 = dev.information.resource.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(subjectId)
                .setFkClass(1)
                .setCommunityVisibility("{ \"toolbox\": false, \"dataApi\": true, \"website\": false}")
                .build();
        infResourceTopic.pipeInput(k2, v2);

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = dev.information.statement.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setFkSubjectInfo(subjectId)
                .setFkProperty(20)
                .setFkObjectInfo(objectId)
                .build();
        infStatementTopic.pipeInput(kS, vS);


        // add dimension
        var k = dev.information.dimension.Key.newBuilder().setPkEntity(objectId).build();
        var v = dev.information.dimension.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(objectId)
                .setFkMeasurementUnit(0)
                .setNumericValue(num)
                .setFkClass(0)
                .build();
        infDimensionTopic.pipeInput(k, v);

        assertThat(statementWithLiteralTopic.isEmpty()).isFalse();
        var outRecords = statementWithLiteralTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kS);
        assertThat(record.getObject().getLabel()).isEqualTo(num + "");
        assertThat(record.getObject().getDimension()).isNotNull();
    }

    @Test
    void testStatementWithCell() {
        long objectId = 30;
        double num = 111;
        int subjectId = 10;

        // add subject entity
        var k2 = dev.information.resource.Key.newBuilder().setPkEntity(subjectId).build();
        var v2 = dev.information.resource.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(subjectId)
                .setFkClass(1)
                .setCommunityVisibility("{ \"toolbox\": false, \"dataApi\": true, \"website\": false}")
                .build();
        infResourceTopic.pipeInput(k2, v2);

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = dev.information.statement.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setFkSubjectInfo(subjectId)
                .setFkProperty(20)
                .setFkObjectTablesCell(objectId)
                .build();
        infStatementTopic.pipeInput(kS, vS);


        // add cell
        var k = dev.tables.cell.Key.newBuilder().setPkCell(objectId).build();
        var v = dev.tables.cell.Value.newBuilder()
                .setEntityVersion(0)
                .setPkCell(objectId)
                .setFkColumn(0)
                .setFkRow(0)
                .setFkDigital(0)
                .setNumericValue(num)
                .setFkClass(0)
                .build();
        tabCellTopic.pipeInput(k, v);

        assertThat(statementWithLiteralTopic.isEmpty()).isFalse();
        var outRecords = statementWithLiteralTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kS);
        assertThat(record.getObject().getCell().getNumericValue()).isEqualTo(num);
    }

    @Test
    void testStatementWithDigital() {
        int objectId = 30;
        int subjectId = 10;

        // add subject entity
        var k2 = dev.information.resource.Key.newBuilder().setPkEntity(subjectId).build();
        var v2 = dev.information.resource.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(subjectId)
                .setFkClass(1)
                .setCommunityVisibility("{ \"toolbox\": false, \"dataApi\": true, \"website\": false}")
                .build();
        infResourceTopic.pipeInput(k2, v2);

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = dev.information.statement.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setFkSubjectInfo(subjectId)
                .setFkProperty(20)
                .setFkObjectData(objectId)
                .build();
        infStatementTopic.pipeInput(kS, vS);


        // add cell
        var k = dev.data.digital.Key.newBuilder().setPkEntity(objectId).build();
        var v = dev.data.digital.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setQuillDoc("")
                .setString("")
                .setEntityVersion(0)
                .setPkEntity(objectId)
                .build();
        datDigitalTopic.pipeInput(k, v);

        assertThat(statementWithLiteralTopic.isEmpty()).isFalse();
        var outRecords = statementWithLiteralTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kS);
        assertThat(record.getObject().getDigital()).isNotNull();
    }

}
