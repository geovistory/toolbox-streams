package org.geovistory.toolbox.streams.entity.processors.community;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CommunityEntityTimeSpanTest {

    private static final String SCHEMA_REGISTRY_SCOPE = CommunityEntityTimeSpanTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<CommunityEntityKey, CommunityEntityTopStatementsValue> communityEntityTopStatementsTopic;

    private TestOutputTopic<CommunityEntityKey, TimeSpanValue> outputTopic;


    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        var nameSupplement = "toolbox";
        Topology topology = CommunityEntityTimeSpan.buildStandalone(new StreamsBuilder(), nameSupplement);

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        communityEntityTopStatementsTopic = testDriver.createInputTopic(
                CommunityEntityTopStatements.getOutputTopicName(nameSupplement),
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.CommunityEntityTopStatementsValue().serializer());


        outputTopic = testDriver.createOutputTopic(
                CommunityEntityTimeSpan.getOutputTopicName(nameSupplement),
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

        var map = new HashMap<String, CommunityTopStatementsWithPropLabelValue>();

        int ongoingThroughout = 71;
        var ongoingThroughoutStatements = CommunityTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setPropertyId(ongoingThroughout)
                .setEntityId(entityId).setIsOutgoing(true).setPropertyLabel("ongoingThroughout").setStatements(List.of(
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


        map.put(ongoingThroughout + "_out", ongoingThroughoutStatements);

        var entityTopStatements = CommunityEntityTopStatementsValue.newBuilder()
                .setEntityId(entityId).setProjectCount(1).setClassId(classId).setMap(map).build();


        var k = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        communityEntityTopStatementsTopic.pipeInput(k, entityTopStatements);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var record = outRecords.get(k);

        assertThat(record.getTimeSpan().getP81().getDuration()).isEqualTo("1 day");
        assertThat(record.getFirstSecond()).isEqualTo(expectedFirstSec);
        assertThat(record.getLastSecond()).isEqualTo(expectedLastSec);
    }

    @Test
    void testTopologyWithoutTemproalData() {
        var classId = 2;
        var entityId = "foo";
        var nonTemporalProperty = 1;

        var map = new HashMap<String, CommunityTopStatementsWithPropLabelValue>();

        var nonTemporalStatements = CommunityTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setPropertyId(nonTemporalProperty)
                .setEntityId(entityId).setIsOutgoing(true).setPropertyLabel("nonTemporalProperty").setStatements(List.of(
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


        map.put(nonTemporalProperty + "_out", nonTemporalStatements);

        var entityTopStatements = CommunityEntityTopStatementsValue.newBuilder()
                .setEntityId(entityId).setProjectCount(1).setClassId(classId).setMap(map).build();


        var k = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        communityEntityTopStatementsTopic.pipeInput(k, entityTopStatements);

        assertThat(outputTopic.isEmpty()).isTrue();
    }


}
