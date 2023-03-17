package org.geovistory.toolbox.streams.fulltext.community;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.fulltext.Env;
import org.geovistory.toolbox.streams.fulltext.I;
import org.geovistory.toolbox.streams.fulltext.processors.community.CommunityEntityFulltext;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CommunityEntityFulltextTest {

    private static final String SCHEMA_REGISTRY_SCOPE = CommunityEntityFulltext.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<CommunityTopStatementsKey, CommunityTopStatementsValue> communityTopStatementsTopic;
    private TestInputTopic<CommunityEntityKey, CommunityEntityLabelConfigValue> communityEntityWithLabelConfigTopic;
    private TestInputTopic<CommunityPropertyLabelKey, CommunityPropertyLabelValue> communityPropertyLabelTopic;
    private TestOutputTopic<CommunityEntityKey, CommunityEntityFulltextValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        var nameSupplement = "toolbox";
        Topology topology = CommunityEntityFulltext.buildStandalone(new StreamsBuilder(), nameSupplement);

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        communityTopStatementsTopic = testDriver.createInputTopic(
                Env.INSTANCE.TOPIC_COMMUNITY_TOP_STATEMENTS,
                avroSerdes.CommunityTopStatementsKey().serializer(),
                avroSerdes.CommunityTopStatementsValue().serializer());

        communityEntityWithLabelConfigTopic = testDriver.createInputTopic(
                Env.INSTANCE.TOPIC_COMMUNITY_ENTITY_WITH_LABEL_CONFIG,
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.CommunityEntityLabelConfigValue().serializer());

        communityPropertyLabelTopic = testDriver.createInputTopic(
                Env.INSTANCE.TOPIC_COMMUNITY_PROPERTY_LABEL,
                avroSerdes.CommunityPropertyLabelKey().serializer(),
                avroSerdes.CommunityPropertyLabelValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                CommunityEntityFulltext.getOutputTopicName(nameSupplement),
                avroSerdes.CommunityEntityKey().deserializer(),
                avroSerdes.CommunityEntityFulltextValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testTopology() {
        var classId = 2;
        var entityId = "foo";
        var propIdSecondPart = 3;
        var propIdFirstPart = 4;
        var kC = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        var vC = CommunityEntityLabelConfigValue.newBuilder().setClassId(classId)
                .setConfig(EntityLabelConfig.newBuilder().setLabelParts(List.of(
                        // second part
                        EntityLabelConfigPart.newBuilder().setOrdNum(2).setField(EntityLabelConfigPartField.newBuilder()
                                .setFkProperty(propIdSecondPart)
                                .setIsOutgoing(true)
                                .setNrOfStatementsInLabel(2).build()).build(),
                        // first part
                        EntityLabelConfigPart.newBuilder().setOrdNum(1).setField(EntityLabelConfigPartField.newBuilder()
                                .setFkProperty(propIdFirstPart)
                                .setIsOutgoing(false)
                                .setNrOfStatementsInLabel(1).build()).build()
                )).build()).build();

        communityEntityWithLabelConfigTopic.pipeInput(kC, vC);


        // Field 1

        var kL1 = CommunityPropertyLabelKey.newBuilder()
                .setClassId(classId)
                .setIsOutgoing(false)
                .setPropertyId(propIdFirstPart)
                .setLanguageId(I.EN.get())
                .build();
        var vL1 = CommunityPropertyLabelValue.newBuilder()
                .setIsOutgoing(false)
                .setPropertyId(propIdFirstPart)
                .setLabel("has friend")
                .build();
        communityPropertyLabelTopic.pipeInput(kL1, vL1);

        var k1 = CommunityTopStatementsKey.newBuilder()
                .setEntityId(entityId)
                .setIsOutgoing(false)
                .setPropertyId(propIdFirstPart)
                .build();
        var v1 = CommunityTopStatementsValue.newBuilder()
                .setClassId(classId).setPropertyId(propIdFirstPart)
                .setEntityId(entityId).setIsOutgoing(false).setStatements(List.of(
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(1f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setObjectLabel("foo")
                                        .setSubjectLabel("Max").build()).build(),
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(2f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setObjectLabel("foo")
                                        .setSubjectLabel("Mia").build()).build()
                )).build();

        communityTopStatementsTopic.pipeInput(k1, v1);


        // Field 2

        var kL2 = CommunityPropertyLabelKey.newBuilder()
                .setClassId(classId)
                .setIsOutgoing(true)
                .setPropertyId(propIdSecondPart)
                .setLanguageId(I.EN.get())
                .build();
        var vL2 = CommunityPropertyLabelValue.newBuilder()
                .setIsOutgoing(true)
                .setPropertyId(propIdSecondPart)
                .setLabel("participates in")
                .build();
        communityPropertyLabelTopic.pipeInput(kL2, vL2);

        var k2 = CommunityTopStatementsKey.newBuilder()

                .setEntityId(entityId)
                .setIsOutgoing(true)
                .setPropertyId(propIdSecondPart)
                .build();
        var v2 = CommunityTopStatementsValue.newBuilder()
                .setClassId(classId).setPropertyId(propIdSecondPart)
                .setEntityId(entityId).setIsOutgoing(true)
                .setStatements(List.of(
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(1f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdSecondPart)
                                        .setSubjectLabel("foo")
                                        .setObjectLabel("Voyage 1").build()).build(),
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(2f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdSecondPart)
                                        .setSubjectLabel("foo")
                                        .setObjectLabel("Voyage 2").build()).build()
                )).build();
        communityTopStatementsTopic.pipeInput(k2, v2);

        // Field 3

        kL1 = CommunityPropertyLabelKey.newBuilder()
                .setClassId(classId)
                .setIsOutgoing(true)
                .setPropertyId(9876543)
                .setLanguageId(I.EN.get())
                .build();
        vL1 = CommunityPropertyLabelValue.newBuilder()
                .setIsOutgoing(true)
                .setPropertyId(9876543)
                .setLabel("has fun with")
                .build();
        communityPropertyLabelTopic.pipeInput(kL1, vL1);

        var k3 = CommunityTopStatementsKey.newBuilder()
                .setEntityId(entityId)
                .setIsOutgoing(true)
                .setPropertyId(propIdSecondPart)
                .build();
        var v3 = CommunityTopStatementsValue.newBuilder()
                .setClassId(classId).setPropertyId(9876543)
                .setEntityId(entityId).setIsOutgoing(true)
                .setStatements(List.of(
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(1f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(9876543)
                                        .setSubjectLabel("foo")
                                        .setObjectLabel("Toy 1").build()).build(),
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(2f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(9876543)
                                        .setSubjectLabel("foo")
                                        .setObjectLabel("Toy 2").build()).build()
                )).build();
        communityTopStatementsTopic.pipeInput(k3, v3);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var k = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        var record = outRecords.get(k);
        assertThat(record.getFulltext()).isEqualTo("has friend: Max, Mia.\nparticipates in: Voyage 1, Voyage 2.\nhas fun with: Toy 1, Toy 2.");
    }

    @Test
    void testTopologyEntityWithoutLabelConfig() {
        var classId = 2;
        var entityId = "foo";
        var propIdSecondPart = 3;
        var propIdFirstPart = 4;


        // Field 1

        var kL1 = CommunityPropertyLabelKey.newBuilder()
                .setClassId(classId)
                .setIsOutgoing(false)
                .setPropertyId(propIdFirstPart)
                .setLanguageId(I.EN.get())
                .build();
        var vL1 = CommunityPropertyLabelValue.newBuilder()
                .setIsOutgoing(false)
                .setPropertyId(propIdFirstPart)
                .setLabel("has friend")
                .build();
        communityPropertyLabelTopic.pipeInput(kL1, vL1);

        var k1 = CommunityTopStatementsKey.newBuilder()
                .setEntityId(entityId)
                .setIsOutgoing(false)
                .setPropertyId(propIdFirstPart)
                .build();
        var v1 = CommunityTopStatementsValue.newBuilder()
                .setClassId(classId).setPropertyId(propIdFirstPart)
                .setEntityId(entityId).setIsOutgoing(false).setStatements(List.of(
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(1f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setObjectLabel("foo")
                                        .setSubjectLabel("Max").build()).build(),
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(2f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setObjectLabel("foo")
                                        .setSubjectLabel("Mia").build()).build()
                )).build();

        communityTopStatementsTopic.pipeInput(k1, v1);


        // Field 2

        var kL2 = CommunityPropertyLabelKey.newBuilder()
                .setClassId(classId)
                .setIsOutgoing(true)
                .setPropertyId(propIdSecondPart)
                .setLanguageId(I.EN.get())
                .build();
        var vL2 = CommunityPropertyLabelValue.newBuilder()
                .setIsOutgoing(true)
                .setPropertyId(propIdSecondPart)
                .setLabel("participates in")
                .build();
        communityPropertyLabelTopic.pipeInput(kL2, vL2);

        var k2 = CommunityTopStatementsKey.newBuilder()

                .setEntityId(entityId)
                .setIsOutgoing(true)
                .setPropertyId(propIdSecondPart)
                .build();
        var v2 = CommunityTopStatementsValue.newBuilder()
                .setClassId(classId).setPropertyId(propIdSecondPart)
                .setEntityId(entityId).setIsOutgoing(true)
                .setStatements(List.of(
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(1f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdSecondPart)
                                        .setSubjectLabel("foo")
                                        .setObjectLabel("Voyage 1").build()).build(),
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(2f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdSecondPart)
                                        .setSubjectLabel("foo")
                                        .setObjectLabel("Voyage 2").build()).build()
                )).build();
        communityTopStatementsTopic.pipeInput(k2, v2);

        // Field 3
        kL1 = CommunityPropertyLabelKey.newBuilder()
                .setClassId(classId)
                .setIsOutgoing(true)
                .setPropertyId(9876543)
                .setLanguageId(I.EN.get())
                .build();
        vL1 = CommunityPropertyLabelValue.newBuilder()
                .setIsOutgoing(true)
                .setPropertyId(9876543)
                .setLabel("has fun with")
                .build();
        communityPropertyLabelTopic.pipeInput(kL1, vL1);

        var k3 = CommunityTopStatementsKey.newBuilder()
                .setEntityId(entityId)
                .setIsOutgoing(true)
                .setPropertyId(propIdSecondPart)
                .build();
        var v3 = CommunityTopStatementsValue.newBuilder()
                .setClassId(classId).setPropertyId(9876543)
                .setEntityId(entityId).setIsOutgoing(true)
                .setStatements(List.of(
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(1f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(9876543)
                                        .setSubjectLabel("foo")
                                        .setObjectLabel("Toy 1").build()).build(),
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(2f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(9876543)
                                        .setSubjectLabel("foo")
                                        .setObjectLabel("Toy 2").build()).build()
                )).build();
        communityTopStatementsTopic.pipeInput(k3, v3);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var k = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        var record = outRecords.get(k);
        assertThat(record.getFulltext()).isEqualTo("participates in: Voyage 1, Voyage 2.\nhas friend: Max, Mia.\nhas fun with: Toy 1, Toy 2.");
    }


    @Test
    void testTopologyEntityWithoutStatements() {
        var classId = 2;
        var entityId = "foo";
        var propIdFirstPart = 4;


        // Field 1

        var kL1 = CommunityPropertyLabelKey.newBuilder()
                .setClassId(classId)
                .setIsOutgoing(false)
                .setPropertyId(propIdFirstPart)
                                .setLanguageId(I.EN.get())
                .build();
        var vL1 = CommunityPropertyLabelValue.newBuilder()
                .setIsOutgoing(false)
                .setPropertyId(propIdFirstPart)
                .setLabel("has friend")
                .build();
        communityPropertyLabelTopic.pipeInput(kL1, vL1);

        var k1 = CommunityTopStatementsKey.newBuilder()
                .setEntityId(entityId)
                .setIsOutgoing(false)
                .setPropertyId(propIdFirstPart)
                .build();
        var v1 = CommunityTopStatementsValue.newBuilder()
                .setClassId(classId).setPropertyId(propIdFirstPart)
                .setEntityId(entityId).setIsOutgoing(false).setStatements(List.of()).build();

        communityTopStatementsTopic.pipeInput(k1, v1);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var k = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        var record = outRecords.get(k);
        assertThat(record.getFulltext()).isEqualTo("");
    }


}
