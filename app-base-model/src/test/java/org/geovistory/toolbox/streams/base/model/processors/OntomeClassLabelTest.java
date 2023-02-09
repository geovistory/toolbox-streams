package org.geovistory.toolbox.streams.base.model.processors;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.OntomeClassLabelKey;
import org.geovistory.toolbox.streams.avro.OntomeClassLabelValue;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class OntomeClassLabelTest {

    private static final String SCHEMA_REGISTRY_SCOPE = OntomeClassLabelTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;

    private TestInputTopic<dev.data_for_history.api_class.Key, dev.data_for_history.api_class.Value> apiClassTopic;
    private TestOutputTopic<OntomeClassLabelKey, OntomeClassLabelValue> ontomeClassLabelTopic;


    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = OntomeClassLabel.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        apiClassTopic = testDriver.createInputTopic(
                OntomeClassLabel.input.TOPICS.api_class,
                avroSerdes.DfhApiClassKey().serializer(),
                avroSerdes.DfhApiClassValue().serializer());


        ontomeClassLabelTopic = testDriver.createOutputTopic(
                OntomeClassLabel.output.TOPICS.ontome_class_label,
                avroSerdes.OntomeClassLabelKey().deserializer(),
                avroSerdes.OntomeClassLabelValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testOntoMeClassLabel() {
        // add class with valid language
        var apKey = new dev.data_for_history.api_class.Key(1);
        var apVal = dev.data_for_history.api_class.Value.newBuilder()
                .setDfhAncestorClasses(new ArrayList<>())
                .setDfhParentClasses(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPkClass(44)
                .setDfhClassLabel("Label with valid lang")
                .setDfhClassLabelLanguage(" de ") // add spaces to test trim
                .build();
        apiClassTopic.pipeInput(apKey, apVal);

        // add class with invalid language
        apKey.setPkEntity(2);
        apVal.setDfhClassLabelLanguage("invalid_lang_przf");
        apVal.setDfhClassLabel("Invalid Valid Label Lang");
        apiClassTopic.pipeInput(apKey, apVal);

        assertThat(ontomeClassLabelTopic.isEmpty()).isFalse();
        var outRecords = ontomeClassLabelTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var classLangKey = OntomeClassLabelKey.newBuilder()
                .setClassId(44)
                .setLanguageId(18605)
                .build();
        assertThat(outRecords.get(classLangKey).getLabel()).isEqualTo("Label with valid lang");
    }

}
