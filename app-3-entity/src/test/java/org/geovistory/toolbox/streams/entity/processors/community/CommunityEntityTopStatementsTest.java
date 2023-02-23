package org.geovistory.toolbox.streams.entity.processors.community;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.Env;
import org.geovistory.toolbox.streams.entity.I;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CommunityEntityTopStatementsTest {

    private static final String SCHEMA_REGISTRY_SCOPE = CommunityEntityTopStatementsTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<CommunityEntityKey, CommunityEntityValue> communityEntityTopic;
    private TestInputTopic<CommunityTopStatementsKey, CommunityTopStatementsValue> communityTopStatementsTopic;
    private TestInputTopic<CommunityPropertyLabelKey, CommunityPropertyLabelValue> communityPropertyLabelTopic;
    private TestOutputTopic<CommunityEntityKey, CommunityEntityTopStatementsValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        var nameSupplement = "toolbox";
        Topology topology = CommunityEntityTopStatements.buildStandalone(new StreamsBuilder(), nameSupplement);

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        communityEntityTopic = testDriver.createInputTopic(
                Env.INSTANCE.TOPIC_COMMUNITY_ENTITY,
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.CommunityEntityValue().serializer());

        communityPropertyLabelTopic = testDriver.createInputTopic(
                Env.INSTANCE.TOPIC_COMMUNITY_PROPERTY_LABEL,
                avroSerdes.CommunityPropertyLabelKey().serializer(),
                avroSerdes.CommunityPropertyLabelValue().serializer());

        communityTopStatementsTopic = testDriver.createInputTopic(
                Env.INSTANCE.TOPIC_COMMUNITY_TOP_STATEMENTS,
                avroSerdes.CommunityTopStatementsKey().serializer(),
                avroSerdes.CommunityTopStatementsValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                CommunityEntityTopStatements.getOutputTopicName(nameSupplement),
                avroSerdes.CommunityEntityKey().deserializer(),
                avroSerdes.CommunityEntityTopStatementsValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testAddingProperties() {

        var entityId = "i1";
        var classId = 2;

        // add project entity
        var kE = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        var vE = CommunityEntityValue.newBuilder().setEntityId(entityId).setProjectCount(1).setClassId(classId).build();
        communityEntityTopic.pipeInput(kE, vE);

        // add top statements
        var kT = CommunityTopStatementsKey.newBuilder().setEntityId(entityId).setPropertyId(1)
                .setIsOutgoing(true).build();
        var vT = CommunityTopStatementsValue.newBuilder().setEntityId(entityId).setPropertyId(1)
                .setIsOutgoing(true).setStatements(
                        List.of(
                                CommunityStatementValue.newBuilder().setStatementId(3).setStatement(
                                        StatementEnrichedValue.newBuilder()
                                                .setSubjectId("foo").setPropertyId(1).setObjectId("bar").build()
                                ).build()
                        )
                ).build();
        communityTopStatementsTopic.pipeInput(kT, vT);

        // add project property label
        var kP = CommunityPropertyLabelKey.newBuilder().setPropertyId(1)
                .setLanguageId(I.EN.get())
                .setIsOutgoing(true)
                .setClassId(classId).build();
        var vP = CommunityPropertyLabelValue.newBuilder().setLabel("has").build();
        communityPropertyLabelTopic.pipeInput(kP, vP);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultKey = CommunityEntityKey.newBuilder()
                .setEntityId(entityId)
                .build();
        var record = outRecords.get(resultKey);
        assertThat(record.getMap().size()).isEqualTo(1);
        assertThat(record.getMap().get("1_out").getStatements().size()).isEqualTo(1);
        assertThat(record.getMap().get("1_out").getPropertyLabel()).isEqualTo("has");
    }


}
