package org.geovistory.toolbox.streams.entity.label.processors.community;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.CommunityTopStatementsKey;
import org.geovistory.toolbox.streams.avro.CommunityTopStatementsValue;
import org.geovistory.toolbox.streams.entity.label.*;
import org.geovistory.toolbox.streams.entity.label.processsors.community.CommunityToolboxTopStatements;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CommunityToolboxTopStatementsTest {

    private static final String SCHEMA_REGISTRY_SCOPE = CommunityToolboxTopStatementsTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<CommunityTopStatementsKey, CommunityTopStatementsValue> outgoingTopStatementsTopic;
    private TestInputTopic<CommunityTopStatementsKey, CommunityTopStatementsValue> incomingTopStatementsTopic;
    private TestOutputTopic<CommunityTopStatementsKey, CommunityTopStatementsValue> outputTopic;

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
        var communityToolboxTopStatements = new CommunityToolboxTopStatements(avroSerdes, registerInputTopic, registerInnerTopic, outputTopicNames);
        communityToolboxTopStatements.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);


        outgoingTopStatementsTopic = testDriver.createInputTopic(
                outputTopicNames.communityToolboxTopOutgoingStatements(),
                avroSerdes.CommunityTopStatementsKey().serializer(),
                avroSerdes.CommunityTopStatementsValue().serializer());


        incomingTopStatementsTopic = testDriver.createInputTopic(
                outputTopicNames.communityToolboxTopIncomingStatements(),
                avroSerdes.CommunityTopStatementsKey().serializer(),
                avroSerdes.CommunityTopStatementsValue().serializer());


        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.communityToolboxTopStatements(),
                avroSerdes.CommunityTopStatementsKey().deserializer(),
                avroSerdes.CommunityTopStatementsValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }


    @Test
    void testMerge() {

        var k = CommunityTopStatementsKey.newBuilder()
                .setPropertyId(1)
                .setEntityId("1")
                .setIsOutgoing(true)
                .build();
        var v = CommunityTopStatementsValue.newBuilder()
                .setStatements(List.of())
                .setPropertyId(1)
                .setEntityId("1")
                .setIsOutgoing(true)
                .build();

        outgoingTopStatementsTopic.pipeInput(k, v);
        k.setIsOutgoing(false);
        v.setIsOutgoing(false);
        incomingTopStatementsTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);

    }

}
