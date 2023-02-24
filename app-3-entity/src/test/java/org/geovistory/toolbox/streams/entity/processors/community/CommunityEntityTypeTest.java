package org.geovistory.toolbox.streams.entity.processors.community;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.Env;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CommunityEntityTypeTest {

    private static final String SCHEMA_REGISTRY_SCOPE = CommunityEntityTypeTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
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
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        var nameSupplement = "toolbox";
        Topology topology = CommunityEntityType.buildStandalone(new StreamsBuilder(), nameSupplement);

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        hasTypePropertyTopic = testDriver.createInputTopic(
                Env.INSTANCE.TOPIC_HAS_TYPE_PROPERTY,
                avroSerdes.HasTypePropertyKey().serializer(),
                avroSerdes.HasTypePropertyValue().serializer());

        communityTopOutgoingStatements = testDriver.createInputTopic(
                Env.INSTANCE.TOPIC_COMMUNITY_TOP_OUTGOING_STATEMENTS,
                avroSerdes.CommunityTopStatementsKey().serializer(),
                avroSerdes.CommunityTopStatementsValue().serializer());

        communityEntityTopic = testDriver.createInputTopic(
                Env.INSTANCE.TOPIC_COMMUNITY_ENTITY,
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.CommunityEntityValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                CommunityEntityType.getOutputTopicName(nameSupplement),
                avroSerdes.CommunityEntityKey().deserializer(),
                avroSerdes.CommunityEntityTypeValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
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
