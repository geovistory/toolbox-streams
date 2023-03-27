package org.geovistory.toolbox.streams.statement.object.processors;


import io.debezium.data.geometry.Geography;
import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.GeoUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class StatementObjectTest {

    private static final String SCHEMA_REGISTRY_SCOPE = StatementObjectTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<NodeKey, NodeValue> nodeTopic;
    private TestInputTopic<dev.information.statement.Key, StatementEnrichedValue> statementWithSubjectTopic;
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

        Topology topology = StatementObject.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();


        statementWithSubjectTopic = testDriver.createInputTopic(
                StatementObject.input.TOPICS.statement_with_subject,
                avroSerdes.InfStatementKey().serializer(),
                avroSerdes.StatementEnrichedValue().serializer()
        );

        nodeTopic = testDriver.createInputTopic(
                StatementObject.input.TOPICS.nodes,
                avroSerdes.NodeKey().serializer(),
                avroSerdes.NodeValue().serializer()
        );

        statementWithEntityTopic = testDriver.createOutputTopic(
                StatementObject.output.TOPICS.statement_with_entity,
                avroSerdes.InfStatementKey().deserializer(),
                avroSerdes.StatementEnrichedValue().deserializer());


        statementWithLiteralTopic = testDriver.createOutputTopic(
                StatementObject.output.TOPICS.statement_with_literal,
                avroSerdes.InfStatementKey().deserializer(),
                avroSerdes.StatementEnrichedValue().deserializer());

        statementOtherTopic = testDriver.createOutputTopic(
                StatementObject.output.TOPICS.statement_with_literal,
                avroSerdes.InfStatementKey().deserializer(),
                avroSerdes.StatementEnrichedValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testStatementWithoutObject() {
        var subjectId = "i10";
        int propertyId = 20;
        var objectId = "i30";

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = StatementEnrichedValue.newBuilder()
                .setSubjectId(subjectId)
                .setPropertyId(propertyId)
                .setObjectId(objectId)
                .build();
        statementWithSubjectTopic.pipeInput(kS, vS);

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
        var vS = StatementEnrichedValue.newBuilder()
                .setSubjectId("i" + subjectId)
                .setPropertyId(propertyId)
                .setObjectId("i" + objectId)
                .build();
        statementWithSubjectTopic.pipeInput(kS, vS);

        // add object entity
        var k = NodeKey.newBuilder().setId("i" + objectId).build();
        var v = NodeValue.newBuilder()
                .setClassId(classId)
                .setEntity(Entity.newBuilder()
                        .setPkEntity(30)
                        .setFkClass(classId)
                        .setCommunityVisibilityToolbox(true)
                        .setCommunityVisibilityDataApi(false)
                        .setCommunityVisibilityWebsite(true)
                        .build())
                .build();
        nodeTopic.pipeInput(k, v);

        assertThat(statementWithEntityTopic.isEmpty()).isFalse();
        var outRecords = statementWithEntityTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kS);
        assertThat(record.getObjectClassId()).isEqualTo(classId);
        assertThat(record.getObject().getEntity().getCommunityVisibilityToolbox()).isEqualTo(true);
        assertThat(record.getObject().getEntity().getCommunityVisibilityDataApi()).isEqualTo(false);
        assertThat(record.getObject().getEntity().getCommunityVisibilityWebsite()).isEqualTo(true);

    }

    @Test
    void testStatementWithAppellation() {
        int objectId = 30;
        int classId = 40;
        int subjectId = 10;
        int propertyId = 20;


        // string with more than 100
        String label = "Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_";

        // string with 100 characters
        String expected = "Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_";

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = StatementEnrichedValue.newBuilder()
                .setSubjectId("i" + subjectId)
                .setPropertyId(propertyId)
                .setObjectId("i" + objectId)
                .build();
        statementWithSubjectTopic.pipeInput(kS, vS);

        // add appellation
        var k = NodeKey.newBuilder().setId("i" + objectId).build();
        var v = NodeValue.newBuilder()
                .setClassId(classId)
                .setLabel(expected)
                .setAppellation(Appellation.newBuilder()
                        .setPkEntity(30)
                        .setFkClass(classId)
                        .setString(label)
                        .build())
                .build();
        nodeTopic.pipeInput(k, v);

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
        int propertyId = 20;
        int classId = 40;

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = StatementEnrichedValue.newBuilder()
                .setSubjectId("i" + subjectId)
                .setPropertyId(propertyId)
                .setObjectId("i" + objectId)
                .build();

        statementWithSubjectTopic.pipeInput(kS, vS);


        // add language

        var k = NodeKey.newBuilder().setId("i" + objectId).build();
        var v = NodeValue.newBuilder()
                .setClassId(classId)
                .setLabel(label)
                .setLanguage(Language.newBuilder()
                        .setPkEntity(30)
                        .setFkClass(classId)
                        .setNotes(label)
                        .setPkLanguage("en")
                        .build())
                .build();
        nodeTopic.pipeInput(k, v);

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
        int propertyId = 20;
        int classId = 40;


        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = StatementEnrichedValue.newBuilder()
                .setSubjectId("i" + subjectId)
                .setPropertyId(propertyId)
                .setObjectId("i" + objectId)
                .build();
        statementWithSubjectTopic.pipeInput(kS, vS);

        // add lang_string
        var k = NodeKey.newBuilder().setId("i" + objectId).build();
        var v = NodeValue.newBuilder()
                .setClassId(classId)
                .setLabel(label)
                .setLangString(LangString.newBuilder()
                        .setPkEntity(30)
                        .setFkClass(classId)
                        .setFkLanguage(0)
                        .setString(label)
                        .build())
                .build();
        nodeTopic.pipeInput(k, v);

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
        int propertyId = 20;
        int classId = 40;

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = StatementEnrichedValue.newBuilder()
                .setSubjectId("i" + subjectId)
                .setPropertyId(propertyId)
                .setObjectId("i" + objectId)
                .build();
        statementWithSubjectTopic.pipeInput(kS, vS);


        // add lang_string
        var k = NodeKey.newBuilder().setId("i" + objectId).build();
        var v = NodeValue.newBuilder()
                .setClassId(classId)
                .setLangString(LangString.newBuilder()
                        .setPkEntity(30)
                        .setFkClass(classId)
                        .setFkLanguage(0)
                        .setString(label)
                        .build())
                .build();
        nodeTopic.pipeInput(k, v);

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
        int propertyId = 20;
        int classId = 40;

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = StatementEnrichedValue.newBuilder()
                .setSubjectId("i" + subjectId)
                .setPropertyId(propertyId)
                .setObjectId("i" + objectId)
                .build();
        statementWithSubjectTopic.pipeInput(kS, vS);

        double x = 1;
        double y = 2;

        // add place
        var k = NodeKey.newBuilder().setId("i" + objectId).build();
        var v = NodeValue.newBuilder()
                .setClassId(classId)
                .setLabel("WGS84: " + x + "°, " + y + "°")
                .setPlace(Place.newBuilder()
                        .setPkEntity(30)
                        .setFkClass(classId)
                        .setGeoPoint(Geography.newBuilder()
                                .setWkb(GeoUtils.pointToBytes(x, y, 4326))
                                .setSrid(4326)
                                .build())
                        .build())
                .build();
        nodeTopic.pipeInput(k, v);


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
        int propertyId = 20;
        int classId = 40;

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = StatementEnrichedValue.newBuilder()
                .setSubjectId("i" + subjectId)
                .setPropertyId(propertyId)
                .setObjectId("i" + objectId)
                .build();
        statementWithSubjectTopic.pipeInput(kS, vS);

        // add time primitive
        var k = NodeKey.newBuilder().setId("i" + objectId).build();
        var v = NodeValue.newBuilder()
                .setClassId(classId)
                .setTimePrimitive(TimePrimitive.newBuilder()
                        .setPkEntity(30)
                        .setFkClass(classId)
                        .setCalendar("gregorian")
                        .setDuration("1 year")
                        .setJulianDay(1234567)
                        .build())
                .build();
        nodeTopic.pipeInput(k, v);

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
        int propertyId = 20;
        int classId = 40;

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = StatementEnrichedValue.newBuilder()
                .setSubjectId("i" + subjectId)
                .setPropertyId(propertyId)
                .setObjectId("i" + objectId)
                .build();
        statementWithSubjectTopic.pipeInput(kS, vS);


        // add dimension
        var k = NodeKey.newBuilder().setId("i" + objectId).build();
        var v = NodeValue.newBuilder()
                .setClassId(classId)
                .setLabel(num + "")
                .setDimension(Dimension.newBuilder()
                        .setPkEntity(30)
                        .setFkClass(classId)
                        .setFkMeasurementUnit(0)
                        .setNumericValue(num)
                        .build())
                .build();
        nodeTopic.pipeInput(k, v);

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
        int propertyId = 20;

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = StatementEnrichedValue.newBuilder()
                .setSubjectId("i" + subjectId)
                .setPropertyId(propertyId)
                .setObjectId("i" + objectId)
                .build();
        statementWithSubjectTopic.pipeInput(kS, vS);


        // add cell
        var k = NodeKey.newBuilder().setId("i" + objectId).build();
        var v = NodeValue.newBuilder()
                .setClassId(0)
                .setCell(Cell.newBuilder()
                        .setPkCell(objectId)
                        .setFkColumn(0)
                        .setFkRow(0)
                        .setFkDigital(0)
                        .setNumericValue(num)
                        .setFkClass(0)
                        .build())
                .build();
        nodeTopic.pipeInput(k, v);

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
        int propertyId = 20;
        int classId = 40;

        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = StatementEnrichedValue.newBuilder()
                .setSubjectId("i" + subjectId)
                .setPropertyId(propertyId)
                .setObjectId("i" + objectId)
                .build();
        statementWithSubjectTopic.pipeInput(kS, vS);

        // add digital
        var k = NodeKey.newBuilder().setId("i" + objectId).build();
        var v = NodeValue.newBuilder()
                .setClassId(classId)
                .setDigital(Digital.newBuilder()
                        .setFkNamespace(2)
                        .setFkSystemType(1)
                        .setPkEntity(objectId)
                        .build())
                .build();
        nodeTopic.pipeInput(k, v);

        assertThat(statementWithLiteralTopic.isEmpty()).isFalse();
        var outRecords = statementWithLiteralTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kS);
        assertThat(record.getObject().getDigital()).isNotNull();
    }

}
