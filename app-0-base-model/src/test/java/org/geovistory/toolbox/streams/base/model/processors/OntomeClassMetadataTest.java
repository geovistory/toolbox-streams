package org.geovistory.toolbox.streams.base.model.processors;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.OntomeClassKey;
import org.geovistory.toolbox.streams.avro.OntomeClassMetadataValue;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
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

    private TestInputTopic<dev.data_for_history.api_class.Key, dev.data_for_history.api_class.Value> apiClassTopic;
    private TestOutputTopic<OntomeClassKey, OntomeClassMetadataValue> outputTopic;


    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = OntomeClassMetadata.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        apiClassTopic = testDriver.createInputTopic(
                OntomeClassMetadata.input.TOPICS.api_class,
                avroSerdes.DfhApiClassKey().serializer(),
                avroSerdes.DfhApiClassValue().serializer());


        outputTopic = testDriver.createOutputTopic(
                OntomeClassMetadata.output.TOPICS.ontome_class_metadata,
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
        var apKey = new dev.data_for_history.api_class.Key(1);
        var apVal = dev.data_for_history.api_class.Value.newBuilder()
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

}
