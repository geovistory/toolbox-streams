package org.geovistory.toolbox.streams.nodes.processors;


import io.debezium.data.geometry.Geography;
import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.NodeKey;
import org.geovistory.toolbox.streams.avro.NodeValue;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.GeoUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class NodesTest {

    private static final String SCHEMA_REGISTRY_SCOPE = NodesTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<dev.information.resource.Key, dev.information.resource.Value> infResourceTopic;
    private TestInputTopic<dev.information.language.Key, dev.information.language.Value> infLanguageTopic;
    private TestInputTopic<dev.information.appellation.Key, dev.information.appellation.Value> infAppellationTopic;
    private TestInputTopic<dev.information.lang_string.Key, dev.information.lang_string.Value> infLangStringTopic;
    private TestInputTopic<dev.information.place.Key, dev.information.place.Value> infPlaceTopic;
    private TestInputTopic<dev.information.time_primitive.Key, dev.information.time_primitive.Value> infTimePrimitiveTopic;
    private TestInputTopic<dev.information.dimension.Key, dev.information.dimension.Value> infDimensionTopic;
    private TestInputTopic<dev.data.digital.Key, dev.data.digital.Value> datDigitalTopic;
    private TestInputTopic<dev.tables.cell.Key, dev.tables.cell.Value> tabCellTopic;
    private TestOutputTopic<NodeKey, NodeValue> nodeTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = Nodes.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        infResourceTopic = testDriver.createInputTopic(
                Nodes.input.TOPICS.inf_resource,
                avroSerdes.InfResourceKey().serializer(),
                avroSerdes.InfResourceValue().serializer()
        );
        infLanguageTopic = testDriver.createInputTopic(
                Nodes.input.TOPICS.inf_language,
                avroSerdes.InfLanguageKey().serializer(),
                avroSerdes.InfLanguageValue().serializer()
        );
        infAppellationTopic = testDriver.createInputTopic(
                Nodes.input.TOPICS.inf_appellation,
                avroSerdes.InfAppellationKey().serializer(),
                avroSerdes.InfAppellationValue().serializer()
        );
        infLangStringTopic = testDriver.createInputTopic(
                Nodes.input.TOPICS.inf_lang_string,
                avroSerdes.InfLangStringKey().serializer(),
                avroSerdes.InfLangStringValue().serializer()
        );
        infPlaceTopic = testDriver.createInputTopic(
                Nodes.input.TOPICS.inf_place,
                avroSerdes.InfPlaceKey().serializer(),
                avroSerdes.InfPlaceValue().serializer()
        );
        infTimePrimitiveTopic = testDriver.createInputTopic(
                Nodes.input.TOPICS.inf_time_primitive,
                avroSerdes.InfTimePrimitiveKey().serializer(),
                avroSerdes.InfTimePrimitiveValue().serializer()
        );
        infDimensionTopic = testDriver.createInputTopic(
                Nodes.input.TOPICS.inf_dimension,
                avroSerdes.InfDimensionKey().serializer(),
                avroSerdes.InfDimensionValue().serializer()
        );
        tabCellTopic = testDriver.createInputTopic(
                Nodes.input.TOPICS.tab_cell,
                avroSerdes.TabCellKey().serializer(),
                avroSerdes.TabCellValue().serializer()
        );
        datDigitalTopic = testDriver.createInputTopic(
                Nodes.input.TOPICS.dat_digital,
                avroSerdes.DatDigitalKey().serializer(),
                avroSerdes.DatDigitalValue().serializer()
        );


        nodeTopic = testDriver.createOutputTopic(
                Nodes.output.TOPICS.nodes,
                avroSerdes.NodeKey().deserializer(),
                avroSerdes.NodeValue().deserializer());


    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testEntity() {
        int id = 10;
        int classId = 40;


        // add entity
        var k2 = dev.information.resource.Key.newBuilder().setPkEntity(id).build();
        var v2 = dev.information.resource.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(id)
                .setFkClass(classId)
                .setCommunityVisibility("{ \"toolbox\": false, \"dataApi\": true, \"website\": false}")
                .build();
        infResourceTopic.pipeInput(k2, v2);

        assertThat(nodeTopic.isEmpty()).isFalse();
        var outRecords = nodeTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(NodeKey.newBuilder().setId("i" + id).build());
        assertThat(record.getClassId()).isEqualTo(classId);

        assertThat(record.getEntity().getCommunityVisibilityToolbox()).isEqualTo(false);
        assertThat(record.getEntity().getCommunityVisibilityDataApi()).isEqualTo(true);
        assertThat(record.getEntity().getCommunityVisibilityWebsite()).isEqualTo(false);
    }

    @Test
    void testAppellation() {
        int id = 30;
        int classId = 40;

        // string with more than 100
        String label = "Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_";

        // string with 100 characters
        String expected = "Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_Ten_Chars_";


        // add appellation
        var k = dev.information.appellation.Key.newBuilder().setPkEntity(id).build();
        var v = dev.information.appellation.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(0)
                .setQuillDoc("")
                .setPkEntity(id)
                .setFkClass(classId)
                .setString(label)
                .build();
        infAppellationTopic.pipeInput(k, v);

        assertThat(nodeTopic.isEmpty()).isFalse();
        var outRecords = nodeTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(NodeKey.newBuilder().setId("i" + id).build());
        assertThat(record.getLabel()).isEqualTo(expected);
        assertThat(record.getAppellation()).isNotNull();
        assertThat(record.getClassId()).isEqualTo(classId);
    }

    @Test
    void testLanguage() {
        int id = 30;
        String label = "English";

        // add language
        var k = dev.information.language.Key.newBuilder().setPkLanguage("en").build();
        var v = dev.information.language.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkLanguage("en")
                .setPkEntity(id)
                .setFkClass(0)
                .setNotes(label)
                .build();
        infLanguageTopic.pipeInput(k, v);

        assertThat(nodeTopic.isEmpty()).isFalse();
        var outRecords = nodeTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(NodeKey.newBuilder().setId("i" + id).build());
        assertThat(record.getLabel()).isEqualTo(label);
        assertThat(record.getLanguage()).isNotNull();
    }

    @Test
    void testLangString() {
        int id = 30;
        String label = "Label";

        // add lang_string
        var k = dev.information.lang_string.Key.newBuilder().setPkEntity(id).build();
        var v = dev.information.lang_string.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(id)
                .setFkLanguage(0)
                .setFkClass(0)
                .setString(label)
                .build();
        infLangStringTopic.pipeInput(k, v);

        assertThat(nodeTopic.isEmpty()).isFalse();
        var outRecords = nodeTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(NodeKey.newBuilder().setId("i" + id).build());
        assertThat(record.getLabel()).isEqualTo(label);
        assertThat(record.getLangString()).isNotNull();
    }

    @Test
    void testNullLangString() {
        int id = 30;
        String label = null;

        // add lang_string
        var k = dev.information.lang_string.Key.newBuilder().setPkEntity(id).build();
        var v = dev.information.lang_string.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(id)
                .setFkLanguage(0)
                .setFkClass(0)
                .setString(label)
                .build();
        infLangStringTopic.pipeInput(k, v);

        assertThat(nodeTopic.isEmpty()).isFalse();
        var outRecords = nodeTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(NodeKey.newBuilder().setId("i" + id).build());
        assertThat(record.getLabel()).isEqualTo(label);
        assertThat(record.getLangString()).isNotNull();
    }

    @Test
    void testPlace() {
        int id = 30;

        double x = 1;
        double y = 2;

        // add place
        var k = dev.information.place.Key.newBuilder().setPkEntity(id).build();
        var v = dev.information.place.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(id)
                .setGeoPoint(Geography.newBuilder()
                        .setWkb(GeoUtils.pointToBytes(x, y, 4326))
                        .setSrid(4326)
                        .build())
                .setFkClass(0)
                .build();
        infPlaceTopic.pipeInput(k, v);

        assertThat(nodeTopic.isEmpty()).isFalse();
        var outRecords = nodeTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(NodeKey.newBuilder().setId("i" + id).build());
        assertThat(record.getLabel()).isEqualTo("WGS84: " + x + "°, " + y + "°");
        assertThat(record.getPlace()).isNotNull();
    }


    @Test
    void testTimePrimitive() {
        int id = 30;

        // add place
        var k = dev.information.time_primitive.Key.newBuilder().setPkEntity(id).build();
        var v = dev.information.time_primitive.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(id)
                .setCalendar("gregorian")
                .setDuration("1 year")
                .setJulianDay(1234567)
                .setFkClass(0)
                .build();
        infTimePrimitiveTopic.pipeInput(k, v);

        assertThat(nodeTopic.isEmpty()).isFalse();
        var outRecords = nodeTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(NodeKey.newBuilder().setId("i" + id).build());
        assertThat(record.getLabel()).isNull();
        assertThat(record.getTimePrimitive()).isNotNull();
    }

    @Test
    void testDimension() {
        int id = 30;
        double num = 111;

        // add dimension
        var k = dev.information.dimension.Key.newBuilder().setPkEntity(id).build();
        var v = dev.information.dimension.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(id)
                .setFkMeasurementUnit(0)
                .setNumericValue(num)
                .setFkClass(0)
                .build();
        infDimensionTopic.pipeInput(k, v);

        assertThat(nodeTopic.isEmpty()).isFalse();
        var outRecords = nodeTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(NodeKey.newBuilder().setId("i" + id).build());
        assertThat(record.getLabel()).isEqualTo(num + "");
        assertThat(record.getDimension()).isNotNull();
    }

    @Test
    void testCell() {
        long id = 30;
        double num = 111;


        // add cell
        var k = dev.tables.cell.Key.newBuilder().setPkCell(id).build();
        var v = dev.tables.cell.Value.newBuilder()
                .setEntityVersion(0)
                .setPkCell(id)
                .setFkColumn(0)
                .setFkRow(0)
                .setFkDigital(0)
                .setNumericValue(num)
                .setFkClass(0)
                .build();
        tabCellTopic.pipeInput(k, v);

        assertThat(nodeTopic.isEmpty()).isFalse();
        var outRecords = nodeTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(NodeKey.newBuilder().setId("t" + id).build());
        assertThat(record.getCell().getNumericValue()).isEqualTo(num);
    }

    @Test
    void testDigital() {
        int id = 30;


        // add cell
        var k = dev.data.digital.Key.newBuilder().setPkEntity(id).build();
        var v = dev.data.digital.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setQuillDoc("")
                .setString("")
                .setEntityVersion(0)
                .setPkEntity(id)
                .build();
        datDigitalTopic.pipeInput(k, v);

        assertThat(nodeTopic.isEmpty()).isFalse();
        var outRecords = nodeTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(NodeKey.newBuilder().setId("d" + id).build());
        assertThat(record.getDigital()).isNotNull();
    }

}
