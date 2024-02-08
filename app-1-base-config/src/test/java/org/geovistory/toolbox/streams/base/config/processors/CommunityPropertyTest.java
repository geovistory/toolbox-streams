package org.geovistory.toolbox.streams.base.config.processors;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.base.config.*;
import org.geovistory.toolbox.streams.lib.ConfiguredAvroSerde;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CommunityPropertyTest {


    private static final String SCHEMA_REGISTRY_SCOPE = CommunityPropertyTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<OntomePropertyKey, OntomePropertyValue> ontomePropertyTopic;
    private TestInputTopic<GeovPropertyLabelKey, GeovPropertyLabelValue> geovPropertyLabelTopic;
    private TestOutputTopic<CommunityPropertyLabelKey, CommunityPropertyLabelValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        var builderSingleton = new BuilderSingleton();
        var as = new ConfiguredAvroSerde();
        as.schemaRegistryUrl = MOCK_SCHEMA_REGISTRY_URL;
        var inputTopicNames = new InputTopicNames();
        var outputTopicNames = new OutputTopicNames();
        var registerInputTopic = new RegisterInputTopic(as, builderSingleton, inputTopicNames);
        var registerInnerTopic = new RegisterInnerTopic(as, builderSingleton, outputTopicNames);
        var communityPropertyLabel = new CommunityPropertyLabel(as, registerInputTopic, registerInnerTopic, outputTopicNames);
        communityPropertyLabel.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);


        ontomePropertyTopic = testDriver.createInputTopic(
                inputTopicNames.ontomeProperty(),
                as.<OntomePropertyKey>key().serializer(),
                as.<OntomePropertyValue>value().serializer());

        geovPropertyLabelTopic = testDriver.createInputTopic(
                outputTopicNames.geovPropertyLabel(),
                as.<GeovPropertyLabelKey>key().serializer(),
                as.<GeovPropertyLabelValue>value().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.communityPropertyLabel(),
                as.<CommunityPropertyLabelKey>key().deserializer(),
                as.<CommunityPropertyLabelValue>value().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testOverrideOntomeLabelWithGeovLabel() {
        // add geov property label
        var gKey = GeovPropertyLabelKey.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setClassId(10)
                .setPropertyId(20)
                .setIsOutgoing(true)
                .setLanguageId(I.DE.get())
                .build();
        var gVal = GeovPropertyLabelValue.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setClassId(10)
                .setPropertyId(20)
                .setIsOutgoing(true)
                .setLanguageId(I.DE.get())
                .setLabel("Geov")
                .setDeleted$1(false)
                .build();
        geovPropertyLabelTopic.pipeInput(gKey, gVal);

        // add ontome property
        var apKey = new OntomePropertyKey(44);
        var apVal = OntomePropertyValue.newBuilder()
                .setDfhAncestorProperties(new ArrayList<>())
                .setDfhParentProperties(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPropertyDomain(10)
                .setDfhPkProperty(20)
                .setDfhPropertyRange(30)
                .setDfhPropertyLabel("Ontome")
                .setDfhPropertyInverseLabel("Ontome Inv")
                .setDfhPropertyLabelLanguage("de")
                .build();
        ontomePropertyTopic.pipeInput(apKey, apVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);
        var k = CommunityPropertyLabelKey.newBuilder()
                .setClassId(10)
                .setPropertyId(20)
                .setIsOutgoing(true)
                .setLanguageId(I.DE.get())
                .build();
        assertThat(outRecords.get(k).getLabel()).isEqualTo("Geov");

         k = CommunityPropertyLabelKey.newBuilder()
                .setClassId(30)
                .setPropertyId(20)
                .setIsOutgoing(false)
                .setLanguageId(I.DE.get())
                .build();
        assertThat(outRecords.get(k).getLabel()).isEqualTo("Ontome Inv");
    }

}
