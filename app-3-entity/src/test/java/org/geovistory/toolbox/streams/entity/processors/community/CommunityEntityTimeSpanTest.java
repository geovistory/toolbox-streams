package org.geovistory.toolbox.streams.entity.processors.community;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.*;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CommunityEntityTimeSpanTest {

    private static final String SCHEMA_REGISTRY_SCOPE = CommunityEntityTimeSpanTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<CommunityTopStatementsKey, CommunityTopStatementsValue> communityTopOutgoingStatementsTopic;

    private TestOutputTopic<CommunityEntityKey, TimeSpanValue> outputTopic;


    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        var builderSingleton = new BuilderSingleton();
        var avroSerdes = new AvroSerdes();
        avroSerdes.QUARKUS_KAFKA_STREAMS_SCHEMA_REGISTRY_URL = MOCK_SCHEMA_REGISTRY_URL;
        var inputTopicNames = new InputTopicNames();
        var outputTopicNames = new OutputTopicNames();
        var registerInputTopic = new RegisterInputTopic(avroSerdes, builderSingleton, inputTopicNames);
        var communityClassLabel = new CommunityEntityTimeSpan(avroSerdes, registerInputTopic, outputTopicNames);
        communityClassLabel.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        communityTopOutgoingStatementsTopic = testDriver.createInputTopic(
                inputTopicNames.communityTopOutgoingStatements,
                avroSerdes.CommunityTopStatementsKey().serializer(),
                avroSerdes.CommunityTopStatementsValue().serializer());


        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.communityEntityTimeSpan(),
                avroSerdes.CommunityEntityKey().deserializer(),
                avroSerdes.TimeSpanValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testTopology() {
        var classId = 2;
        var entityId = "foo";
        long expectedFirstSec = 204139785600L;
        long expectedLastSec = 204139871999L;


        int ongoingThroughout = 71;
        var k = CommunityTopStatementsKey.newBuilder()
                .setEntityId(entityId)
                .setPropertyId(ongoingThroughout)
                .setIsOutgoing(true)
                .build();
        var v = CommunityTopStatementsValue.newBuilder()
                .setClassId(classId).setPropertyId(ongoingThroughout)
                .setEntityId(entityId).setIsOutgoing(true).setStatements(List.of(
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(1f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(ongoingThroughout)
                                        .setObject(NodeValue.newBuilder()
                                                .setClassId(0)
                                                .setTimePrimitive(TimePrimitive.newBuilder()
                                                        .setJulianDay(2362729)
                                                        .setDuration("1 day")
                                                        .setCalendar("gregorian")
                                                        .build())
                                                .build()
                                        ).build()
                                ).build()
                )).build();


        communityTopOutgoingStatementsTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var entityKey = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        var record = outRecords.get(entityKey);

        assertThat(record.getTimeSpan().getP81().getDuration()).isEqualTo("1 day");
        assertThat(record.getFirstSecond()).isEqualTo(expectedFirstSec);
        assertThat(record.getLastSecond()).isEqualTo(expectedLastSec);
    }

    @Test
    void testTopologyWithoutTemproalData() {
        var classId = 2;
        var entityId = "foo";
        var nonTemporalProperty = 1;

        var k = CommunityTopStatementsKey.newBuilder()
                .setEntityId(entityId)
                .setPropertyId(nonTemporalProperty)
                .setIsOutgoing(true)
                .build();

        var v = CommunityTopStatementsValue.newBuilder()
                .setClassId(classId).setPropertyId(nonTemporalProperty)
                .setEntityId(entityId).setIsOutgoing(true).setStatements(List.of(
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(1f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(nonTemporalProperty)
                                        .setObject(NodeValue.newBuilder()
                                                .setClassId(0)
                                                .setTimePrimitive(TimePrimitive.newBuilder()
                                                        .setJulianDay(2362729)
                                                        .setDuration("1 day")
                                                        .setCalendar("gregorian")
                                                        .build())
                                                .build()
                                        ).build()
                                ).build()
                )).build();


        communityTopOutgoingStatementsTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isTrue();
    }


}
