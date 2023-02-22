package org.geovistory.toolbox.streams.entity.label.processors.community;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.CommunityTopStatementsKey;
import org.geovistory.toolbox.streams.avro.CommunityTopStatementsValue;
import org.geovistory.toolbox.streams.entity.label.processsors.community.CommunityToolboxTopStatements;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
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
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = CommunityToolboxTopStatements.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        outgoingTopStatementsTopic = testDriver.createInputTopic(
                CommunityToolboxTopStatements.input.TOPICS.community_toolbox_top_outgoing_statements,
                avroSerdes.CommunityTopStatementsKey().serializer(),
                avroSerdes.CommunityTopStatementsValue().serializer());


        incomingTopStatementsTopic = testDriver.createInputTopic(
                CommunityToolboxTopStatements.input.TOPICS.community_toolbox_top_incoming_statements,
                avroSerdes.CommunityTopStatementsKey().serializer(),
                avroSerdes.CommunityTopStatementsValue().serializer());


        outputTopic = testDriver.createOutputTopic(
                CommunityToolboxTopStatements.output.TOPICS.community_toolbox_top_statements,
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
