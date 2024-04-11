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
class CommunityEntityTypeTest {
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
    private TestInputTopic<HasTypePropertyKey, HasTypePropertyValue> hasTypePropertyTopic;
    private TestInputTopic<CommunityEntityKey, CommunityEntityValue> communityEntityTopic;
    private TestInputTopic<CommunityTopStatementsKey, CommunityTopStatementsValue> communityTopOutgoingStatements;
    private TestOutputTopic<CommunityEntityKey, CommunityEntityTypeValue> outputTopic;

    @BeforeEach
    void setup() {

        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");

        testDriver = new TopologyTestDriver(topology, props);


        hasTypePropertyTopic = testDriver.createInputTopic(
                inputTopicNames.getHasTypeProperty(),
                as.kS(),
                as.vS());

        communityTopOutgoingStatements = testDriver.createInputTopic(
                inputTopicNames.getCommunityEdges(),
                as.kS(),
                as.vS());

        communityEntityTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityEntity(),
                as.kS(),
                as.vS());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.communityEntityType(),
                as.kD(),
                as.vD());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
        FileRemover.removeDir(this.stateDir);
    }

    @Test
    void testCommunityEntityType() {

        var entityId = "i1";
        var classId = 3;


        // add an entity
        var kE = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        var vE = CommunityEntityValue.newBuilder().setEntityId(entityId).setClassId(3).build();
        communityEntityTopic.pipeInput(kE, vE);

        var kC = HasTypePropertyKey.newBuilder().setClassId(classId).build();
        var vC = HasTypePropertyValue.newBuilder()
                .setClassId(classId)
                .setPropertyId(987)
                .build();
        hasTypePropertyTopic.pipeInput(kC, vC);


        var kS = CommunityTopStatementsKey.newBuilder()
                .setEntityId(entityId)
                .setPropertyId(987).setIsOutgoing(true).build();
        var vS = CommunityTopStatementsValue.newBuilder()
                .setEntityId(entityId)
                .setPropertyId(987).setIsOutgoing(true)
                .setStatements(List.of(
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(1f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setPropertyId(987)
                                        .setObjectId("i654")
                                        .setObjectLabel("Joy")
                                        .build()).build()
                )).build();
        communityTopOutgoingStatements.pipeInput(kS, vS);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = CommunityEntityKey.newBuilder()
                .setEntityId(entityId)

                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getTypeId()).isEqualTo("i654");
        assertThat(record.getTypeLabel()).isEqualTo("Joy");

    }

    @Test
    void testCommunityEntityTypeDeleteHasTypeProp() {

        var entityId = "i1";
        var classId = 3;


        // add an entity
        var kE = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        var vE = CommunityEntityValue.newBuilder().setEntityId(entityId).setClassId(3).build();
        communityEntityTopic.pipeInput(kE, vE);

        var kC = HasTypePropertyKey.newBuilder().setClassId(classId).build();
        var vC = HasTypePropertyValue.newBuilder()
                .setClassId(classId)
                .setPropertyId(987)
                .build();
        hasTypePropertyTopic.pipeInput(kC, vC);


        var kS = CommunityTopStatementsKey.newBuilder()
                .setEntityId(entityId)
                .setPropertyId(987).setIsOutgoing(true).build();
        var vS = CommunityTopStatementsValue.newBuilder()
                .setEntityId(entityId)
                .setPropertyId(987).setIsOutgoing(true)
                .setStatements(List.of(
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(1f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setPropertyId(987)
                                        .setObjectId("i654")
                                        .setObjectLabel("Joy")
                                        .build()).build()
                )).build();
        communityTopOutgoingStatements.pipeInput(kS, vS);

        vC.setDeleted$1(true);
        hasTypePropertyTopic.pipeInput(kC, vC);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = CommunityEntityKey.newBuilder()
                .setEntityId(entityId)

                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getDeleted$1()).isEqualTo(true);

    }

    @Test
    void testCommunityEntityTypeDeleteStatement() {

        var entityId = "i1";
        var classId = 3;


        // add an entity
        var kE = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        var vE = CommunityEntityValue.newBuilder().setEntityId(entityId).setClassId(3).build();
        communityEntityTopic.pipeInput(kE, vE);

        var kC = HasTypePropertyKey.newBuilder().setClassId(classId).build();
        var vC = HasTypePropertyValue.newBuilder()
                .setClassId(classId)
                .setPropertyId(987)
                .build();
        hasTypePropertyTopic.pipeInput(kC, vC);


        var kS = CommunityTopStatementsKey.newBuilder()
                .setEntityId(entityId)
                .setPropertyId(987).setIsOutgoing(true).build();
        var vS = CommunityTopStatementsValue.newBuilder()
                .setEntityId(entityId)
                .setPropertyId(987).setIsOutgoing(true)
                .setStatements(List.of(
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(1f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setPropertyId(987)
                                        .setObjectId("i654")
                                        .setObjectLabel("Joy")
                                        .build()).build()
                )).build();

        communityTopOutgoingStatements.pipeInput(kS, vS);

        vS.setStatements(List.of());
        communityTopOutgoingStatements.pipeInput(kS, vS);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = CommunityEntityKey.newBuilder()
                .setEntityId(entityId)

                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getDeleted$1()).isEqualTo(true);

    }

}
