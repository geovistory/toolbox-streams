package org.geovistory.toolbox.streams.entity.label.processors.community;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label.processsors.community.CommunityToolboxTopOutgoingStatements;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CommunityToolboxTopOutgoingStatementsTest {

    private static final String SCHEMA_REGISTRY_SCOPE = CommunityToolboxTopOutgoingStatementsTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<CommunityStatementKey, CommunityStatementValue> projectStatementTopic;
    private TestInputTopic<CommunityEntityKey, CommunityEntityLabelValue> projectEntityLabelTopic;
    private TestOutputTopic<CommunityTopStatementsKey, CommunityTopStatementsValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = CommunityToolboxTopOutgoingStatements.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        projectStatementTopic = testDriver.createInputTopic(
                CommunityToolboxTopOutgoingStatements.input.TOPICS.community_toolbox_statement,
                avroSerdes.CommunityStatementKey().serializer(),
                avroSerdes.CommunityStatementValue().serializer());

        projectEntityLabelTopic = testDriver.createInputTopic(
                CommunityToolboxTopOutgoingStatements.input.TOPICS.community_toolbox_entity_label,
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.CommunityEntityLabelValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                CommunityToolboxTopOutgoingStatements.output.TOPICS.community_toolbox_top_outgoing_statements,
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
        int propertyId = 20;
        String objectId = "i30";
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
                .setProjectCount(1)
                .setStatementId(3)
                .setStatement(StatementEnrichedValue.newBuilder().setPropertyId(propertyId)
                        .setSubjectId(subjectId).setSubject(subject)
                        .setObjectId(objectId).setObject(object).build())
                .setAvgOrdNumOfRange(3f)
                .build();
        projectStatementTopic.pipeInput(k, v);

        v.setStatementId(1);
        v.setAvgOrdNumOfRange(1f);

        projectStatementTopic.pipeInput(k, v);

        v.setStatementId(2);
        v.setAvgOrdNumOfRange(2f);
        projectStatementTopic.pipeInput(k, v);

        v.setStatementId(0);
        v.setAvgOrdNumOfRange(0f);
        projectStatementTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultKey = CommunityTopStatementsKey.newBuilder()
                .setEntityId(subjectId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .build();
        var record = outRecords.get(resultKey);
        assertThat(record.getStatements().size()).isEqualTo(4);
        assertThat(record.getStatements().get(2).getAvgOrdNumOfRange()).isEqualTo(2);
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
                .setProjectCount(1)
                .setStatement(StatementEnrichedValue.newBuilder().setPropertyId(propertyId)
                        .setSubjectId(subjectId).setSubject(subject)
                        .setObjectId(objectId).setObject(object).build())
                .setAvgOrdNumOfRange(3f)
                .build();
        projectStatementTopic.pipeInput(k, v);

        // add subject entity label
        var kSE = CommunityEntityKey.newBuilder().setEntityId(subjectId).build();
        var vSE = CommunityEntityLabelValue.newBuilder().setEntityId(subjectId)
                .setLabelSlots(List.of("")).setLabel("Jack").build();
        projectEntityLabelTopic.pipeInput(kSE, vSE);

        // add object entity label
        var kOE = CommunityEntityKey.newBuilder().setEntityId(objectId).build();
        var vOE = CommunityEntityLabelValue.newBuilder().setEntityId(objectId)
                .setLabelSlots(List.of("")).setLabel("Maria").build();
        projectEntityLabelTopic.pipeInput(kOE, vOE);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = CommunityTopStatementsKey.newBuilder()
                .setIsOutgoing(true)
                .setEntityId(subjectId)
                .setPropertyId(propertyId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getStatements().get(0).getStatement().getSubjectLabel()).isEqualTo(null);
        assertThat(record.getStatements().get(0).getStatement().getObjectLabel()).isEqualTo("Maria");
    }
}
