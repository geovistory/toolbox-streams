package org.geovistory.toolbox.streams.base.config.processors;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.GeovClassLabelKey;
import org.geovistory.toolbox.streams.avro.GeovClassLabelValue;
import org.geovistory.toolbox.streams.base.config.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class GeovClassLabelTest {

    private static final String SCHEMA_REGISTRY_SCOPE = GeovClassLabelTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;

    private TestInputTopic<ts.projects.text_property.Key, ts.projects.text_property.Value> textPropertyTopic;
    private TestOutputTopic<GeovClassLabelKey, GeovClassLabelValue> geovClassLabelTopic;


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
        var registerInnerTopic = new RegisterInnerTopic(avroSerdes, builderSingleton, outputTopicNames);
        var geovClassLabel = new GeovClassLabel(avroSerdes, registerInputTopic, registerInnerTopic,outputTopicNames);
        geovClassLabel.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        textPropertyTopic = testDriver.createInputTopic(
                inputTopicNames.proTextProperty(),
                avroSerdes.ProTextPropertyKey().serializer(),
                avroSerdes.ProTextPropertyValue().serializer());


        geovClassLabelTopic = testDriver.createOutputTopic(
                outputTopicNames.geovClassLabel(),
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
        var k = new ts.projects.text_property.Key(1);
        var v = ts.projects.text_property.Value.newBuilder()
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
