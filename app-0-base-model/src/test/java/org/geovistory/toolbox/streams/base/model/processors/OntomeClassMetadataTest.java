package org.geovistory.toolbox.streams.base.model.processors;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.OntomeClassKey;
import org.geovistory.toolbox.streams.avro.OntomeClassMetadataValue;
import org.geovistory.toolbox.streams.base.model.AvroSerdes;
import org.geovistory.toolbox.streams.base.model.BuilderSingleton;
import org.geovistory.toolbox.streams.base.model.InputTopicNames;
import org.geovistory.toolbox.streams.base.model.OutputTopicNames;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class OntomeClassMetadataTest {

    private static final String SCHEMA_REGISTRY_SCOPE = OntomeClassMetadataTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;

    private TestInputTopic<ts.data_for_history.api_class.Key, ts.data_for_history.api_class.Value> apiClassTopic;
    private TestOutputTopic<OntomeClassKey, OntomeClassMetadataValue> outputTopic;


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
        var ontomeClassMetadata = new OntomeClassMetadata(avroSerdes, builderSingleton, inputTopicNames, outputTopicNames);
        ontomeClassMetadata.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        apiClassTopic = testDriver.createInputTopic(
                ontomeClassMetadata.inApiClass(),
                avroSerdes.DfhApiClassKey().serializer(),
                avroSerdes.DfhApiClassValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.ontomeClassMetadata(),
                avroSerdes.OntomeClassKey().deserializer(),
                avroSerdes.OntomeClassMetadataValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testOntoMeClassMetadata() {

        var parentClasses = List.of(1, 2, 3);
        var ancestorClasses = List.of(4, 5, 6);

        // add class
        var apKey = new ts.data_for_history.api_class.Key(1);
        var apVal = ts.data_for_history.api_class.Value.newBuilder()
                .setDfhPkClass(44)
                .setDfhParentClasses(parentClasses)
                .setDfhAncestorClasses(new ArrayList<>())
                .build();
        apiClassTopic.pipeInput(apKey, apVal);

        apVal.setDfhAncestorClasses(ancestorClasses);
        apiClassTopic.pipeInput(apKey, apVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var key = OntomeClassKey.newBuilder()
                .setClassId(44)
                .build();
        assertThat(outRecords.get(key).getParentClasses()).contains(1, 2, 3);
        assertThat(outRecords.get(key).getAncestorClasses()).contains(4, 5, 6);
    }

    @Test
    void shouldOmitDuplicates() {

        var parentClasses = List.of(1, 2, 3);
        var ancestorClasses = List.of(4, 5, 6);

        // add class 1
        var apKey = new ts.data_for_history.api_class.Key(1);
        var apVal = ts.data_for_history.api_class.Value.newBuilder()
                .setDfhAncestorClasses(ancestorClasses)
                .setDfhParentClasses(parentClasses)
                .setDfhFkProfile(97)
                .setDfhPkClass(44)
                .setDfhClassLabel("Klasse 1")
                .setDfhClassLabelLanguage("de")
                .build();
        apiClassTopic.pipeInput(apKey, apVal);

        // update class 1
        apVal.setDfhClassLabel("Class 1");
        apVal.setDfhClassLabelLanguage("en");

        apiClassTopic.pipeInput(apKey, apVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readRecordsToList();
        assertThat(outRecords).hasSize(1);
        assertThat(outRecords.get(0).getValue().getAncestorClasses()).isEqualTo(ancestorClasses);

    }

}
