package org.geovistory.toolbox.streams.entity.label.processors.community;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label.*;
import org.geovistory.toolbox.streams.entity.label.processsors.community.CommunityToolboxEntityLabel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CommunityEntityLabelTest {

    private static final String SCHEMA_REGISTRY_SCOPE = CommunityEntityLabelTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<CommunityEntityLabelConfigKey, CommunityEntityLabelConfigValue> communityEntityLabelConfigTopic;
    private TestInputTopic<CommunityEntityKey, CommunityEntityValue> communityToolboxEntityTopic;
    private TestInputTopic<CommunityTopStatementsKey, CommunityTopStatementsValue> communityToolboxTopStatements;
    private TestOutputTopic<CommunityEntityKey, CommunityEntityLabelValue> outputTopic;

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
        var communityToolboxEntityLabel = new CommunityToolboxEntityLabel(avroSerdes, registerInputTopic, registerInnerTopic, outputTopicNames);
        communityToolboxEntityLabel.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        communityEntityLabelConfigTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityEntityLabelConfig(),
                avroSerdes.CommunityEntityLabelConfigKey().serializer(),
                avroSerdes.CommunityEntityLabelConfigValue().serializer());

        communityToolboxTopStatements = testDriver.createInputTopic(
                outputTopicNames.communityToolboxTopStatements(),
                avroSerdes.CommunityTopStatementsKey().serializer(),
                avroSerdes.CommunityTopStatementsValue().serializer());

        communityToolboxEntityTopic = testDriver.createInputTopic(
                outputTopicNames.communityToolboxEntity(),
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.CommunityEntityValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.communityToolboxEntityLabel(),
                avroSerdes.CommunityEntityKey().deserializer(),
                avroSerdes.CommunityEntityLabelValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }


    /**
     * The goal of this test is to compose the following entity_label: "S1, S2, S3".
     */
    @Test
    void testCommunityToolboxEntityLabel() {

        var entityId = "i1";
        var classId = 3;

        var propIdFirstPart = 5;
        var propIdSecondPart = 4;

        var expected = "S1, S2, S3";

        // add an entity
        var kE = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        var vE = CommunityEntityValue.newBuilder().setEntityId(entityId).setClassId(3).setProjectCount(1).build();
        communityToolboxEntityTopic.pipeInput(kE, vE);
        /*
         * The entity label configuration is:
         * - First Part: property_it = 5, is_outgoing = false, number_of_statements = 1
         * - Second Part: property_it = 4, is_outgoing = true, number_of_statements = 2
         */
        var kC = CommunityEntityLabelConfigKey.newBuilder().setClassId(classId).build();
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

        communityEntityLabelConfigTopic.pipeInput(kC, vC);
        /*
         * Statements for the first part:
         * - subject_id = i1, property_id = 5, object_id = i1, ord_num_for_domain = 1, subject_label = S1
         * - subject_id = i1, property_id = 5, object_id = i1, ord_num_for_domain = 2, subject_label = NOISE
         */
        var kS = CommunityTopStatementsKey.newBuilder()
                .setEntityId(entityId)
                .setPropertyId(propIdFirstPart).setIsOutgoing(false).build();
        var vS = CommunityTopStatementsValue.newBuilder()
                .setEntityId(entityId)
                .setPropertyId(propIdFirstPart).setIsOutgoing(false)
                .setStatements(List.of(
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(1f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setSubjectLabel("S1").build()).build(),
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(2f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setSubjectLabel("NOISE").build()).build()
                )).build();
        communityToolboxTopStatements.pipeInput(kS, vS);

        /*
         * Statements for the Second part:
         * - subject_id = i1, property_id = 4, object_id = i1, ord_num_for_range = 1, object_label = S2
         * - subject_id = i1, property_id = 4, object_id = i1, ord_num_for_range = 2, object_label = S3
         * - subject_id = i1, property_id = 4, object_id = i1, ord_num_for_range = 3, object_label = NOISE
         */
        kS = CommunityTopStatementsKey.newBuilder()
                .setEntityId(entityId)
                .setPropertyId(propIdSecondPart).setIsOutgoing(true).build();
        vS = CommunityTopStatementsValue.newBuilder()
                .setEntityId(entityId)
                .setPropertyId(propIdSecondPart).setIsOutgoing(true)
                .setStatements(List.of(
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfRange(1f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdSecondPart)
                                        .setObjectLabel("S2").build()).build(),
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfRange(2f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdSecondPart)
                                        .setObjectLabel("S3").build()).build(),
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfRange(3f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdSecondPart)
                                        .setObjectLabel("NOISE").build()).build()
                )).build();
        communityToolboxTopStatements.pipeInput(kS, vS);

        /*
         * Add the same statements again to see if it still behaves correctly
         */
        communityToolboxTopStatements.pipeInput(kS, vS);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = CommunityEntityKey.newBuilder()
                .setEntityId(entityId)

                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getLabel()).isEqualTo(expected);

    }

    @Test
    void testShouldAllowNrOfStatementsInLabelBiggerThanStatements() {

        var entityId = "i1";
        var classId = 3;

        var propIdFirstPart = 5;

        var expected = "S1";

        // add an entity
        var kE = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        var vE = CommunityEntityValue.newBuilder().setEntityId(entityId).setClassId(3).setProjectCount(1).build();
        communityToolboxEntityTopic.pipeInput(kE, vE);
        /*
         * The entity label configuration is:
         * - property_it = 5, is_outgoing = false, number_of_statements = 3
         */
        var kC = CommunityEntityLabelConfigKey.newBuilder().setClassId(classId).build();
        var vC = CommunityEntityLabelConfigValue.newBuilder().setClassId(classId)
                .setConfig(EntityLabelConfig.newBuilder().setLabelParts(List.of(
                        EntityLabelConfigPart.newBuilder().setOrdNum(1).setField(EntityLabelConfigPartField.newBuilder()
                                .setFkProperty(propIdFirstPart)
                                .setIsOutgoing(false)
                                .setNrOfStatementsInLabel(3).build()).build()
                )).build()).build();

        communityEntityLabelConfigTopic.pipeInput(kC, vC);
        /*
         * Statements for the first part:
         * - subject_id = i1, property_id = 5, object_id = i1, ord_num_for_domain = 1, subject_label = S1
         */
        var kS = CommunityTopStatementsKey.newBuilder()
                .setEntityId(entityId)
                .setPropertyId(propIdFirstPart).setIsOutgoing(false).build();
        var vS = CommunityTopStatementsValue.newBuilder()
                .setEntityId(entityId)
                .setPropertyId(propIdFirstPart).setIsOutgoing(false)
                .setStatements(List.of(
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(1f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setSubjectLabel("S1").build()).build()
                )).build();
        communityToolboxTopStatements.pipeInput(kS, vS);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = CommunityEntityKey.newBuilder()
                .setEntityId(entityId)

                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getLabel()).isEqualTo(expected);

    }


}
