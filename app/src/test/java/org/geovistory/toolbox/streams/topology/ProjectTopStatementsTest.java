package org.geovistory.toolbox.streams.topology;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.ProjectTopStatementsKey;
import org.geovistory.toolbox.streams.avro.ProjectTopStatementsValue;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.topologies.ProjectTopStatements;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectTopStatementsTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectTopStatementsTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectTopStatementsKey, ProjectTopStatementsValue> outgoingTopStatementsTopic;
    private TestInputTopic<ProjectTopStatementsKey, ProjectTopStatementsValue> incomingTopStatementsTopic;
    private TestOutputTopic<ProjectTopStatementsKey, ProjectTopStatementsValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = ProjectTopStatements.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        outgoingTopStatementsTopic = testDriver.createInputTopic(
                ProjectTopStatements.input.TOPICS.project_top_outgoing_statements,
                avroSerdes.ProjectTopStatementsKey().serializer(),
                avroSerdes.ProjectTopStatementsValue().serializer());


        incomingTopStatementsTopic = testDriver.createInputTopic(
                ProjectTopStatements.input.TOPICS.project_top_incoming_statements,
                avroSerdes.ProjectTopStatementsKey().serializer(),
                avroSerdes.ProjectTopStatementsValue().serializer());


        outputTopic = testDriver.createOutputTopic(
                ProjectTopStatements.output.TOPICS.project_top_statements,
                avroSerdes.ProjectTopStatementsKey().deserializer(),
                avroSerdes.ProjectTopStatementsValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }


    @Test
    void testMerge() {

        var k = ProjectTopStatementsKey.newBuilder()
                .setPropertyId(1)
                .setProjectId(1)
                .setEntityId("1")
                .setIsOutgoing(true)
                .build();
        var v = ProjectTopStatementsValue.newBuilder()
                .setStatements(List.of())
                .setPropertyId(1)
                .setProjectId(1)
                .setEntityId("1")
                .setIsOutgoing(true)
                .build();

        outgoingTopStatementsTopic.pipeInput(k, v);
        k.setProjectId(2);
        v.setProjectId(2);
        incomingTopStatementsTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);

    }

}
