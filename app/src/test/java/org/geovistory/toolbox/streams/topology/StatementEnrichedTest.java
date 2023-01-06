package org.geovistory.toolbox.streams.topology;


import io.debezium.data.geometry.Geography;
import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.StatementEnrichedKey;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.GeoUtils;
import org.geovistory.toolbox.streams.topologies.StatementEnriched;
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
    private TestInputTopic<dev.information.language.Key, dev.information.language.Value> infLanguageTopic;
    private TestInputTopic<dev.information.appellation.Key, dev.information.appellation.Value> infAppellationTopic;
    private TestInputTopic<dev.information.lang_string.Key, dev.information.lang_string.Value> infLangStringTopic;
    private TestInputTopic<dev.information.place.Key, dev.information.place.Value> infPlaceTopic;
    private TestInputTopic<dev.information.time_primitive.Key, dev.information.time_primitive.Value> infTimePrimitiveTopic;
    private TestInputTopic<dev.information.dimension.Key, dev.information.dimension.Value> infDimensionTopic;
    private TestOutputTopic<StatementEnrichedKey, StatementEnrichedValue> outputTopic;

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


        outputTopic = testDriver.createOutputTopic(
                StatementEnriched.output.TOPICS.statement_enriched,
                avroSerdes.StatementEnrichedKey().deserializer(),
                avroSerdes.StatementEnrichedValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testStatementWithoutLiteral() {
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
                .setFkSubjectInfo(10)
                .setFkProperty(20)
                .setFkObjectInfo(objectId)
                .build();
        infStatementTopic.pipeInput(kS, vS);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = StatementEnrichedKey.newBuilder()
                .setSubjectId(subjectId)
                .setPropertyId(propertyId)
                .setObjectId(objectId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getObjectLiteral()).isNull();
    }

    @Test
    void testStatementWithAppellation() {
        int subjectId = 10;
        int propertyId = 20;
        int objectId = 30;
        String label = "Lyon";
        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = dev.information.statement.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setFkSubjectInfo(10)
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
                .setFkClass(0)
                .setString(label)
                .build();
        infAppellationTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = StatementEnrichedKey.newBuilder()
                .setSubjectId(subjectId)
                .setPropertyId(propertyId)
                .setObjectId(objectId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getObjectLiteral().getLabel()).isEqualTo(label);
        assertThat(record.getObjectLiteral().getAppellation()).isNotNull();
    }

    @Test
    void testStatementWithLanguage() {
        int subjectId = 10;
        int propertyId = 20;
        int objectId = 30;
        String label = "English";
        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = dev.information.statement.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setFkSubjectInfo(10)
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

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = StatementEnrichedKey.newBuilder()
                .setSubjectId(subjectId)
                .setPropertyId(propertyId)
                .setObjectId(objectId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getObjectLiteral().getLabel()).isEqualTo(label);
        assertThat(record.getObjectLiteral().getLanguage()).isNotNull();
    }

    @Test
    void testStatementWithLangString() {
        int subjectId = 10;
        int propertyId = 20;
        int objectId = 30;
        String label = "Label";
        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = dev.information.statement.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setFkSubjectInfo(10)
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

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = StatementEnrichedKey.newBuilder()
                .setSubjectId(subjectId)
                .setPropertyId(propertyId)
                .setObjectId(objectId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getObjectLiteral().getLabel()).isEqualTo(label);
        assertThat(record.getObjectLiteral().getLangString()).isNotNull();
    }

    @Test
    void testStatementWithPlace() {
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
                .setFkSubjectInfo(10)
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
                        .setWkb(GeoUtils.pointToBytes(x, y,4326))
                        .setSrid(4326)
                        .build())
                .setFkClass(0)
                .build();
        infPlaceTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = StatementEnrichedKey.newBuilder()
                .setSubjectId(subjectId)
                .setPropertyId(propertyId)
                .setObjectId(objectId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getObjectLiteral().getLabel()).isEqualTo("WGS84: " + x + "°, " + y + "°");
        assertThat(record.getObjectLiteral().getPlace()).isNotNull();
    }


    @Test
    void testStatementWithTimePrimitive() {
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
                .setFkSubjectInfo(10)
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

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = StatementEnrichedKey.newBuilder()
                .setSubjectId(subjectId)
                .setPropertyId(propertyId)
                .setObjectId(objectId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getObjectLiteral().getLabel()).isEqualTo("todo");
        assertThat(record.getObjectLiteral().getTimePrimitive()).isNotNull();
    }

    @Test
    void testStatementWithDimension() {
        int subjectId = 10;
        int propertyId = 20;
        int objectId = 30;
        double num = 111;
        // add statement
        var kS = dev.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = dev.information.statement.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setFkSubjectInfo(10)
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

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = StatementEnrichedKey.newBuilder()
                .setSubjectId(subjectId)
                .setPropertyId(propertyId)
                .setObjectId(objectId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getObjectLiteral().getLabel()).isEqualTo(num + "");
        assertThat(record.getObjectLiteral().getDimension()).isNotNull();
    }

}
