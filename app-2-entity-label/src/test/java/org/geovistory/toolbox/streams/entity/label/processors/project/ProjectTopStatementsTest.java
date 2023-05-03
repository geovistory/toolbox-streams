package org.geovistory.toolbox.streams.entity.label.processors.project;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.ProjectTopStatementsKey;
import org.geovistory.toolbox.streams.avro.ProjectTopStatementsValue;
import org.geovistory.toolbox.streams.entity.label.*;
import org.geovistory.toolbox.streams.entity.label.processsors.project.ProjectTopStatements;
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

        var builderSingleton = new BuilderSingleton();
        var avroSerdes = new AvroSerdes();
        avroSerdes.QUARKUS_KAFKA_STREAMS_SCHEMA_REGISTRY_URL = MOCK_SCHEMA_REGISTRY_URL;
        var inputTopicNames = new InputTopicNames();
        var outputTopicNames = new OutputTopicNames();
        var registerInputTopic = new RegisterInputTopic(avroSerdes, builderSingleton, inputTopicNames);
        var registerInnerTopic = new RegisterInnerTopic(avroSerdes, builderSingleton, outputTopicNames);
        var projectTopStatements = new ProjectTopStatements(avroSerdes, registerInputTopic, registerInnerTopic, outputTopicNames);
        projectTopStatements.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);


        outgoingTopStatementsTopic = testDriver.createInputTopic(
                outputTopicNames.projectTopOutgoingStatements(),
                avroSerdes.ProjectTopStatementsKey().serializer(),
                avroSerdes.ProjectTopStatementsValue().serializer());


        incomingTopStatementsTopic = testDriver.createInputTopic(
                outputTopicNames.projectTopIncomingStatements(),
                avroSerdes.ProjectTopStatementsKey().serializer(),
                avroSerdes.ProjectTopStatementsValue().serializer());


        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectTopStatements(),
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
                .setEdges(List.of())
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
