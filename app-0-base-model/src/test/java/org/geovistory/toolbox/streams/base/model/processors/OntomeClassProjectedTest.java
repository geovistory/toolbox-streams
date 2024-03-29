package org.geovistory.toolbox.streams.base.model.processors;


import ts.data_for_history.api_class.Key;
import ts.data_for_history.api_class.Value;
import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.OntomeClassKey;
import org.geovistory.toolbox.streams.avro.OntomeClassValue;
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

class OntomeClassProjectedTest {

    private static final String SCHEMA_REGISTRY_SCOPE = OntomeClassProjectedTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;

    private TestInputTopic<ts.data_for_history.api_class.Key, ts.data_for_history.api_class.Value> dfhApiClassTopic;
    private TestOutputTopic<OntomeClassKey, OntomeClassValue> outputTopic;
    private Topology topology;
    ProjectedTableRegistrar<
            Key,
            Value,
            OntomeClassKey,
            OntomeClassValue
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
        registrar = new OntomeClassProjected().getRegistrar(avroSerdes,
                builderSingleton,
                inputTopicNames,
                outputTopicNames);
        registrar.addSink();
        topology = registrar.builder.build();

        testDriver = new TopologyTestDriver(topology, props);


        dfhApiClassTopic = testDriver.createInputTopic(
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

        // add class 1
        var apKey = new ts.data_for_history.api_class.Key(1);
        var apVal = ts.data_for_history.api_class.Value.newBuilder()
                .setDfhAncestorClasses(new ArrayList<>())
                .setDfhParentClasses(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPkClass(44)
                .setDfhClassLabel("Class 1")
                .setDfhClassLabelLanguage("de")
                .build();
        dfhApiClassTopic.pipeInput(apKey, apVal);

        // add class 2
        apKey.setPkEntity(2);
        apVal.setDfhPkClass(43);
        apVal.setDfhClassLabel("Class 2");
        dfhApiClassTopic.pipeInput(apKey, apVal);

        // re-add class 2
        dfhApiClassTopic.pipeInput(apKey, apVal);

        // update class 1
        apVal.setDfhPkClass(44);
        dfhApiClassTopic.pipeInput(apKey, apVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readRecordsToList();
        assertThat(outRecords).hasSize(3);
        assertThat(outRecords.get(0).getValue().getDfhPkClass()).isEqualTo(44);
        assertThat(outRecords.get(1).getValue().getDfhPkClass()).isEqualTo(43);
        assertThat(outRecords.get(2).getValue().getDfhPkClass()).isEqualTo(44);


    }

}
