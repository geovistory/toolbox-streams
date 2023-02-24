package org.geovistory.toolbox.streams.base.config.processors;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.GeovClassLabelKey;
import org.geovistory.toolbox.streams.avro.GeovClassLabelValue;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class GeovClassLabelTest {

    private static final String SCHEMA_REGISTRY_SCOPE = GeovClassLabelTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;

    private TestInputTopic<dev.projects.text_property.Key, dev.projects.text_property.Value> textPropertyTopic;
    private TestOutputTopic<GeovClassLabelKey, GeovClassLabelValue> geovClassLabelTopic;


    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = GeovClassLabel.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        textPropertyTopic = testDriver.createInputTopic(
                GeovClassLabel.input.TOPICS.text_property,
                avroSerdes.ProTextPropertyKey().serializer(),
                avroSerdes.ProTextPropertyValue().serializer());


        geovClassLabelTopic = testDriver.createOutputTopic(
                GeovClassLabel.output.TOPICS.geov_class_label,
                avroSerdes.GeovClassLabelKey().deserializer(),
                avroSerdes.GeovClassLabelValue().deserializer());


    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testGeovClassLabel() {
        // add class label
        var k = new dev.projects.text_property.Key(1);
        var v = dev.projects.text_property.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setQuillDoc("")
                .setEntityVersion(0)
                .setFkProject(10)
                .setFkDfhClass(20)
                .setFkLanguage(18605)
                .setFkSystemType(639)
                .setString("Class Label")
                .build();
        textPropertyTopic.pipeInput(k, v);

        // add class scope note
        k.setPkEntity(2);
        v.setFkSystemType(638);
        v.setString("Class Scope Note");
        textPropertyTopic.pipeInput(k, v);

        // add text_property not for class
        k.setPkEntity(2);
        v.setFkDfhClass(null);
        v.setFkProProject(44);
        v.setString("Project Label");
        textPropertyTopic.pipeInput(k, v);


        assertThat(geovClassLabelTopic.isEmpty()).isFalse();
        var outRecords = geovClassLabelTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var classLangKey = GeovClassLabelKey.newBuilder()
                .setProjectId(10)
                .setClassId(20)
                .setLanguageId(18605)
                .build();
        assertThat(outRecords.get(classLangKey).getLabel()).isEqualTo("Class Label");
    }

}
