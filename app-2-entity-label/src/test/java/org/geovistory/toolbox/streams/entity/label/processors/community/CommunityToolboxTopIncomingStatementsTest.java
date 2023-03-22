package org.geovistory.toolbox.streams.entity.label.processors.community;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label.processsors.community.CommunityToolboxTopIncomingStatements;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CommunityToolboxTopIncomingStatementsTest {

    private static final String SCHEMA_REGISTRY_SCOPE = CommunityToolboxTopIncomingStatementsTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<CommunityStatementKey, CommunityStatementValue> communityToolboxStatementWithEntityTopic;

    private TestInputTopic<CommunityEntityKey, CommunityEntityLabelValue> communityToolboxEntityLabelTopic;
    private TestOutputTopic<CommunityTopStatementsKey, CommunityTopStatementsValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = CommunityToolboxTopIncomingStatements.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        communityToolboxStatementWithEntityTopic = testDriver.createInputTopic(
                CommunityToolboxTopIncomingStatements.input.TOPICS.community_toolbox_statement_with_entity,
                avroSerdes.CommunityStatementKey().serializer(),
                avroSerdes.CommunityStatementValue().serializer());

        communityToolboxEntityLabelTopic = testDriver.createInputTopic(
                CommunityToolboxTopIncomingStatements.input.TOPICS.community_toolbox_entity_label,
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.CommunityEntityLabelValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                CommunityToolboxTopIncomingStatements.output.TOPICS.community_toolbox_top_incoming_statements,
                avroSerdes.CommunityTopStatementsKey().deserializer(),
                avroSerdes.CommunityTopStatementsValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testFourStatementsOfSameSubjectAndProperty() {
        String subjectId = "i10";
        NodeValue subject = NodeValue.newBuilder().setLabel("").setId(subjectId).setClassId(0)
                .setEntity(Entity.newBuilder().setFkClass(1)
                        .setCommunityVisibilityWebsite(false)
                        .setCommunityVisibilityDataApi(false)
                        .setCommunityVisibilityToolbox(true).build()).build();
        int propertyId = 20;
        String objectId = "i30";
        NodeValue object = NodeValue.newBuilder().setLabel("").setId(objectId).setClassId(0)
                .setEntity(Entity.newBuilder().setFkClass(1)
                        .setCommunityVisibilityWebsite(false)
                        .setCommunityVisibilityDataApi(false)
                        .setCommunityVisibilityToolbox(true).build()).build();

        // add subject entity label
        var kOE = CommunityEntityKey.newBuilder().setEntityId(subjectId).build();
        var vOE = CommunityEntityLabelValue.newBuilder().setEntityId(subjectId)
                .setLabelSlots(List.of("")).setLabel("Maria").build();
        communityToolboxEntityLabelTopic.pipeInput(kOE, vOE);

        // add statement
        var k = CommunityStatementKey.newBuilder()
                .setStatementId(1)
                .build();
        var v = CommunityStatementValue.newBuilder()
                .setStatementId(3)
                .setStatement(StatementEnrichedValue.newBuilder().setPropertyId(propertyId)
                        .setSubjectClassId(7)
                        .setObjectClassId(8)
                        .setSubjectId(subjectId).setSubject(subject)
                        .setObjectId(objectId).setObject(object).build())
                .setAvgOrdNumOfDomain(3f)
                .setProjectCount(1)
                .build();
        communityToolboxStatementWithEntityTopic.pipeInput(k, v);

        v.setStatementId(1);
        v.setAvgOrdNumOfDomain(1f);

        communityToolboxStatementWithEntityTopic.pipeInput(k, v);

        v.setStatementId(2);
        v.setAvgOrdNumOfDomain(2f);
        communityToolboxStatementWithEntityTopic.pipeInput(k, v);

        v.setStatementId(0);
        v.setAvgOrdNumOfDomain(0f);
        communityToolboxStatementWithEntityTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultKey = CommunityTopStatementsKey.newBuilder()
                .setEntityId(objectId)
                .setPropertyId(propertyId)
                .setIsOutgoing(false)
                .build();
        var record = outRecords.get(resultKey);
        assertThat(record.getStatements().size()).isEqualTo(4);
        assertThat(record.getStatements().get(2).getAvgOrdNumOfDomain()).isEqualTo(2);
        assertThat(record.getClassId()).isEqualTo(8);
    }


    @Test
    void testJoinEntityLabels() {
        var propertyId = 30;
        var subjectId = "i1";
        var objectId = "i2";
        NodeValue subject = NodeValue.newBuilder().setLabel("").setId(subjectId).setClassId(0)
                .setEntity(Entity.newBuilder().setFkClass(1)
                        .setCommunityVisibilityWebsite(false)
                        .setCommunityVisibilityDataApi(false)
                        .setCommunityVisibilityToolbox(true).build()).build();
        NodeValue object = NodeValue.newBuilder().setLabel("").setId(objectId).setClassId(0)
                .setEntity(Entity.newBuilder().setFkClass(1)
                        .setCommunityVisibilityWebsite(false)
                        .setCommunityVisibilityDataApi(false)
                        .setCommunityVisibilityToolbox(true).build()).build();
        // add statement
        var k = CommunityStatementKey.newBuilder()
                .setStatementId(1)
                .build();
        var v = CommunityStatementValue.newBuilder()
                .setStatementId(3)
                .setStatement(StatementEnrichedValue.newBuilder().setPropertyId(propertyId)
                        .setSubjectId(subjectId).setSubject(subject)
                        .setObjectId(objectId).setObject(object).build())
                .setAvgOrdNumOfRange(3f)
                .setProjectCount(1)
                .build();
        communityToolboxStatementWithEntityTopic.pipeInput(k, v);

        // add subject entity label
        var kSE = CommunityEntityKey.newBuilder().setEntityId(subjectId).build();
        var vSE = CommunityEntityLabelValue.newBuilder().setEntityId(subjectId)
                .setLabelSlots(List.of("")).setLabel("Jack").build();
        communityToolboxEntityLabelTopic.pipeInput(kSE, vSE);

        // add object entity label
        var kOE = CommunityEntityKey.newBuilder().setEntityId(objectId).build();
        var vOE = CommunityEntityLabelValue.newBuilder().setEntityId(objectId)
                .setLabelSlots(List.of("")).setLabel("Maria").build();
        communityToolboxEntityLabelTopic.pipeInput(kOE, vOE);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = CommunityTopStatementsKey.newBuilder()
                .setIsOutgoing(false)
                .setEntityId(objectId)
                .setPropertyId(propertyId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getStatements().get(0).getStatement().getSubjectLabel()).isEqualTo("Jack");
        assertThat(record.getStatements().get(0).getStatement().getObjectLabel()).isEqualTo(null);
    }
}
