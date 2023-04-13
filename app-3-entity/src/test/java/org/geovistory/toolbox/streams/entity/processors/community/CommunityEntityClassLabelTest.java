package org.geovistory.toolbox.streams.entity.processors.community;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CommunityEntityClassLabelTest {

    private static final String SCHEMA_REGISTRY_SCOPE = CommunityEntityClassLabelTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<CommunityEntityKey, CommunityEntityValue> communityEntityTopic;
    private TestInputTopic<OntomeClassLabelKey, CommunityClassLabelValue> communityClassLabelTopic;
    private TestOutputTopic<CommunityEntityKey, CommunityEntityClassLabelValue> outputTopic;

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
        var communityClassLabel = new CommunityEntityClassLabel(avroSerdes, registerInputTopic, outputTopicNames);
        communityClassLabel.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        communityClassLabelTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityClassLabel(),
                avroSerdes.OntomeClassLabelKey().serializer(),
                avroSerdes.CommunityClassLabelValue().serializer());

        communityEntityTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityEntity(),
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.CommunityEntityValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.communityEntityClassLabel(),
                avroSerdes.CommunityEntityKey().deserializer(),
                avroSerdes.CommunityEntityClassLabelValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }


    @Test
    void testCommunityEntityClassLabel() {

        var entityId = "i1";
        var classId = 3;
        var classLabel = "my_class";

        // add an entity
        var kE = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        var vE = CommunityEntityValue.newBuilder().setEntityId(entityId).setProjectCount(1).setClassId(3).build();
        communityEntityTopic.pipeInput(kE, vE);

        // add a class label
        var kS = OntomeClassLabelKey.newBuilder().setClassId(classId)
                .setLanguageId(I.EN.get())
                .build();
        var vS = CommunityClassLabelValue.newBuilder()
                .setLabel(classLabel).build();
        communityClassLabelTopic.pipeInput(kS, vS);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kE);
        assertThat(record.getClassLabel()).isEqualTo(classLabel);

    }


    @Test
    void testProjectCount() {

        var entityId = "i1";
        var classId = 3;
        var classLabel = "my_class";

        // add an entity
        var kE = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        var vE = CommunityEntityValue.newBuilder().setEntityId(entityId).setProjectCount(1).setClassId(3).build();
        communityEntityTopic.pipeInput(kE, vE);
        vE.setProjectCount(0);
        communityEntityTopic.pipeInput(kE, vE);

        // add a class label
        var kS = OntomeClassLabelKey.newBuilder().setClassId(classId)
                .setLanguageId(I.EN.get())
                .build();
        var vS = CommunityClassLabelValue.newBuilder()
                .setLabel(classLabel).build();
        communityClassLabelTopic.pipeInput(kS, vS);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kE);
        assertThat(record.getProjectCount()).isEqualTo(0);

    }
}
