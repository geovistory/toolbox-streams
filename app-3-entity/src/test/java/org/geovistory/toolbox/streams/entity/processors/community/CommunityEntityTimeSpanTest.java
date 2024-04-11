package org.geovistory.toolbox.streams.entity.processors.community;


import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.kafka.streams.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.InputTopicNames;
import org.geovistory.toolbox.streams.entity.OutputTopicNames;
import org.geovistory.toolbox.streams.testlib.FileRemover;
import org.geovistory.toolbox.streams.testlib.TopologyTestDriverProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
class CommunityEntityTimeSpanTest {
    @Inject
    Topology topology;

    @Inject
    ConfiguredAvroSerde as;

    @Inject
    OutputTopicNames outputTopicNames;
    @Inject
    InputTopicNames inputTopicNames;
    @ConfigProperty(name = "kafka-streams.state.dir")
    public String stateDir;
    private TopologyTestDriver testDriver;
    private TestInputTopic<CommunityTopStatementsKey, CommunityTopStatementsValue> communityTopOutgoingStatementsTopic;

    private TestOutputTopic<CommunityEntityKey, TimeSpanValue> outputTopic;


    @BeforeEach
    void setup() {

        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");

        testDriver = new TopologyTestDriver(topology, props);

        testDriver = new TopologyTestDriver(topology, props);

        communityTopOutgoingStatementsTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityTopOutgoingStatements(),
                as.kS(),
                as.vS());


        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.communityEntityTimeSpan(),
                as.kD(),
                as.vD());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
        FileRemover.removeDir(this.stateDir);
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
