package org.geovistory.toolbox.streams.base.model.processors;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.HasTypePropertyKey;
import org.geovistory.toolbox.streams.avro.HasTypePropertyValue;
import org.geovistory.toolbox.streams.base.model.BuilderSingleton;
import org.geovistory.toolbox.streams.base.model.InputTopicNames;
import org.geovistory.toolbox.streams.base.model.OutputTopicNames;
import org.geovistory.toolbox.streams.base.model.Prop;
import org.geovistory.toolbox.streams.lib.ConfiguredAvroSerde;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class HasTypePropertyTest {
    private static final String SCHEMA_REGISTRY_SCOPE = HasTypePropertyTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<dev.data_for_history.api_property.Key, dev.data_for_history.api_property.Value> apiPropertyTopic;
    private TestOutputTopic<HasTypePropertyKey, HasTypePropertyValue> ontomePropertyLabelTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");

        var builderSingleton = new BuilderSingleton();
        ConfiguredAvroSerde as = new ConfiguredAvroSerde();
        as.schemaRegistryUrl = MOCK_SCHEMA_REGISTRY_URL;
        var inputTopicNames = new InputTopicNames();
        var outputTopicNames = new OutputTopicNames();
        var hasTypeProperty = new HasTypeProperty(as, builderSingleton, inputTopicNames, outputTopicNames);
        hasTypeProperty.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        apiPropertyTopic = testDriver.createInputTopic(
                hasTypeProperty.inApiProperty(),
                as.<dev.data_for_history.api_property.Key>key().serializer(),
                as.<dev.data_for_history.api_property.Value>value().serializer());


        ontomePropertyLabelTopic = testDriver.createOutputTopic(
                outputTopicNames.hasTypeProperty(),
                as.<HasTypePropertyKey>key().deserializer(),
                as.<HasTypePropertyValue>value().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testAddingThreeProperties() {
        // add has type property for class 10
        var apKey = new dev.data_for_history.api_property.Key(1);
        var apVal = dev.data_for_history.api_property.Value.newBuilder()
                .setDfhAncestorProperties(List.of(123, Prop.HAS_TYPE.get())) // <- remark !
                .setDfhParentProperties(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPropertyDomain(10)
                .setDfhPkProperty(40)
                .setDfhPropertyLabel("has type")
                .setDfhPropertyInverseLabel("is type of")
                .setDfhPropertyLabelLanguage(" de ") // add spaces to test trim
                .setRemovedFromApi(false)
                .setDeleted$1("false")
                .build();
        apiPropertyTopic.pipeInput(apKey, apVal);

        // add has type property for class 11
        apKey.setPkEntity(2);
        apVal.setDfhPropertyDomain(11);
        apVal.setDfhAncestorProperties(List.of(7));
        apVal.setDfhParentProperties((List.of(123, Prop.HAS_TYPE.get()))); // <- remark !
        apVal.setDfhPkProperty(41);

        apiPropertyTopic.pipeInput(apKey, apVal);


        // add has type property for class 12
        apKey.setPkEntity(3);
        apVal.setDfhPropertyDomain(12);
        apVal.setDfhAncestorProperties(List.of(7));
        apVal.setDfhParentProperties(List.of(7));
        apVal.setDfhPkProperty(Prop.HAS_TYPE.get()); // <- remark !
        apiPropertyTopic.pipeInput(apKey, apVal);

        assertThat(ontomePropertyLabelTopic.isEmpty()).isFalse();
        var outRecords = ontomePropertyLabelTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(3);

        assertThat(outRecords.get(HasTypePropertyKey.newBuilder().setClassId(10).build()).getPropertyId()).isEqualTo(40);
        assertThat(outRecords.get(HasTypePropertyKey.newBuilder().setClassId(11).build()).getPropertyId()).isEqualTo(41);
        assertThat(outRecords.get(HasTypePropertyKey.newBuilder().setClassId(12).build()).getPropertyId()).isEqualTo(2);
        assertThat(outRecords.get(HasTypePropertyKey.newBuilder().setClassId(12).build()).getDeleted$1()).isEqualTo(false);
    }

    @Test
    void testDisableProperty() {
        // add has type property for class 10
        var apKey = new dev.data_for_history.api_property.Key(1);
        var apVal = dev.data_for_history.api_property.Value.newBuilder()
                .setDfhAncestorProperties(List.of(123, Prop.HAS_TYPE.get())) // <- remark !
                .setDfhParentProperties(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPropertyDomain(10)
                .setDfhPkProperty(40)
                .setDfhPropertyLabel("has type")
                .setDfhPropertyInverseLabel("is type of")
                .setDfhPropertyLabelLanguage(" de ") // add spaces to test trim
                .setRemovedFromApi(false)
                .build();
        apiPropertyTopic.pipeInput(apKey, apVal);

        // disable
        apVal.setRemovedFromApi(true);
        apiPropertyTopic.pipeInput(apKey, apVal);


        assertThat(ontomePropertyLabelTopic.isEmpty()).isFalse();
        var outRecords = ontomePropertyLabelTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        assertThat(outRecords.get(HasTypePropertyKey.newBuilder().setClassId(10).build()).getDeleted$1()).isTrue();

    }

}
