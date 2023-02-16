package org.geovistory.toolbox.streams.base.model.processors;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.OntomePropertyLabelKey;
import org.geovistory.toolbox.streams.avro.OntomePropertyLabelValue;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class OntomePropertyLabelTest {

    private static final String SCHEMA_REGISTRY_SCOPE = OntomePropertyLabelTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;

    private TestInputTopic<dev.data_for_history.api_property.Key, dev.data_for_history.api_property.Value> apiPropertyTopic;
    private TestOutputTopic<OntomePropertyLabelKey, OntomePropertyLabelValue> ontomePropertyLabelTopic;


    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = OntomePropertyLabel.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        apiPropertyTopic = testDriver.createInputTopic(
                OntomePropertyLabel.input.TOPICS.api_property,
                avroSerdes.DfhApiPropertyKey().serializer(),
                avroSerdes.DfhApiPropertyValue().serializer());


        ontomePropertyLabelTopic = testDriver.createOutputTopic(
                OntomePropertyLabel.output.TOPICS.ontome_property_label,
                avroSerdes.OntomePropertyLabelKey().deserializer(),
                avroSerdes.OntomePropertyLabelValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testOntoMePropertyLabel() {
        // add property with valid language
        var apKey = new dev.data_for_history.api_property.Key(1);
        var apVal = dev.data_for_history.api_property.Value.newBuilder()
                .setDfhAncestorProperties(new ArrayList<>())
                .setDfhParentProperties(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPkProperty(44)
                .setDfhPropertyLabel("Label with valid lang")
                .setDfhPropertyInverseLabel("Inverse Label with valid lang")
                .setDfhPropertyLabelLanguage(" de ") // add spaces to test trim
                .build();
        apiPropertyTopic.pipeInput(apKey, apVal);

        // add property with invalid language
        apKey.setPkEntity(2);
        apVal.setDfhPropertyLabelLanguage("invalid_lang_przf");
        apVal.setDfhPropertyLabel("Invalid Valid Label Lang");
        apiPropertyTopic.pipeInput(apKey, apVal);

        assertThat(ontomePropertyLabelTopic.isEmpty()).isFalse();
        var outRecords = ontomePropertyLabelTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var propertyLangKey = OntomePropertyLabelKey.newBuilder()
                .setPropertyId(44)
                .setLanguageId(18605)
                .build();
        assertThat(outRecords.get(propertyLangKey).getLabel()).isEqualTo("Label with valid lang");
        assertThat(outRecords.get(propertyLangKey).getInverseLabel()).isEqualTo("Inverse Label with valid lang");
    }

}
