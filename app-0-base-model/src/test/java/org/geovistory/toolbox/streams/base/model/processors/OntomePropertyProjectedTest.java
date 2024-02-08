package org.geovistory.toolbox.streams.base.model.processors;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.OntomePropertyKey;
import org.geovistory.toolbox.streams.avro.OntomePropertyValue;
import org.geovistory.toolbox.streams.base.model.AvroSerdes;
import org.geovistory.toolbox.streams.base.model.BuilderSingleton;
import org.geovistory.toolbox.streams.base.model.InputTopicNames;
import org.geovistory.toolbox.streams.base.model.OutputTopicNames;
import org.geovistory.toolbox.streams.lib.ProjectedTableRegistrar;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class OntomePropertyProjectedTest {

    private static final String SCHEMA_REGISTRY_SCOPE = OntomePropertyProjectedTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;

    private TestInputTopic<ts.data_for_history.api_property.Key, ts.data_for_history.api_property.Value> dfhApiPropertyTopic;
    private TestOutputTopic<OntomePropertyKey, OntomePropertyValue> outputTopic;
    private Topology topology;
    ProjectedTableRegistrar<
            ts.data_for_history.api_property.Key,
            ts.data_for_history.api_property.Value,
            OntomePropertyKey,
            OntomePropertyValue
            > registrar;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        var inputTopicNames = new InputTopicNames();
        var outputTopicNames = new OutputTopicNames();
        var builderSingleton = new BuilderSingleton();
        var avroSerdes = new AvroSerdes();
        avroSerdes.QUARKUS_KAFKA_STREAMS_SCHEMA_REGISTRY_URL = MOCK_SCHEMA_REGISTRY_URL;
        registrar = new OntomePropertyProjected().getRegistrar(
                avroSerdes,
                builderSingleton,
                inputTopicNames,
                outputTopicNames
        );

        registrar.addSink();

        topology = registrar.builder.build();

        testDriver = new TopologyTestDriver(topology, props);



        dfhApiPropertyTopic = testDriver.createInputTopic(
                registrar.inputTopicName,
                registrar.inputKeySerde.serializer(),
                registrar.inputValueSerde.serializer());


        outputTopic = testDriver.createOutputTopic(
                registrar.outputTopicName,
                registrar.outputKeySerde.deserializer(),
                registrar.outputValueSerde.deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void topologyShouldContainOutputTopicName() {
        var description = topology.describe();
        var sOne = description.subtopologies().iterator().next();
        var sOneNodes = sOne.nodes().iterator();
        var sOneNodeOne = sOneNodes.next();
        sOneNodes.next();
        sOneNodes.next();
        var sOneNodeFour = sOneNodes.next();

        assertThat(sOneNodeOne.name()).isEqualTo(registrar.baseName + "-source");
        assertThat(sOneNodeFour.toString()).contains(registrar.outputTopicName);

    }

    @Test
    void shouldOmitDuplicates() {

        // add property 1
        var apKey = new ts.data_for_history.api_property.Key(1);
        var apVal = ts.data_for_history.api_property.Value.newBuilder()
                .setDfhAncestorProperties(new ArrayList<>())
                .setDfhParentProperties(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPkProperty(44)
                .setDfhPropertyLabel("Property 1")
                .setDfhPropertyLabelLanguage("de")
                .build();
        dfhApiPropertyTopic.pipeInput(apKey, apVal);

        // add property 2
        apKey.setPkEntity(2);
        apVal.setDfhPkProperty(43);
        apVal.setDfhPropertyLabel("Property 2");
        dfhApiPropertyTopic.pipeInput(apKey, apVal);

        // re-add property 2
        dfhApiPropertyTopic.pipeInput(apKey, apVal);

        // update property 1
        apVal.setDfhPkProperty(44);
        dfhApiPropertyTopic.pipeInput(apKey, apVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readRecordsToList();
        assertThat(outRecords).hasSize(3);
        assertThat(outRecords.get(0).getValue().getDfhPkProperty()).isEqualTo(44);
        assertThat(outRecords.get(1).getValue().getDfhPkProperty()).isEqualTo(43);
        assertThat(outRecords.get(2).getValue().getDfhPkProperty()).isEqualTo(44);


    }

}
