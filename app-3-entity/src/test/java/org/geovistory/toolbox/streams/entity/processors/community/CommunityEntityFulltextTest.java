package org.geovistory.toolbox.streams.entity.processors.community;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.Env;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CommunityEntityFulltextTest {

    private static final String SCHEMA_REGISTRY_SCOPE = CommunityEntityFulltextTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<CommunityEntityKey, CommunityEntityTopStatementsValue> commnuityEntityTopStatementsTopic;
    private TestInputTopic<OntomeClassKey, CommunityEntityLabelConfigValue> commnuityEntityLabelConfigTopic;

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

        commnuityEntityTopStatementsTopic = testDriver.createInputTopic(
                CommunityEntityTopStatements.getOutputTopicName(nameSupplement),
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.CommunityEntityTopStatementsValue().serializer());

        commnuityEntityLabelConfigTopic = testDriver.createInputTopic(
                Env.INSTANCE.TOPIC_COMMUNITY_ENTITY_LABEL_CONFIG,
                avroSerdes.OntomeClassKey().serializer(),
                avroSerdes.CommunityEntityLabelConfigValue().serializer());

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
    void testCreateFulltextMethod() {
        var classId = 2;
        var entityId = "foo";
        var propIdSecondPart = 3;
        var propIdFirstPart = 4;
        var labelConfig = CommunityEntityLabelConfigValue.newBuilder().setClassId(classId)
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

        var map = new HashMap<String, CommunityTopStatementsWithPropLabelValue>();

        var in = CommunityTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setPropertyId(propIdFirstPart)
                .setEntityId(entityId).setIsOutgoing(true).setPropertyLabel("has friend").setStatements(List.of(
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

        var out = CommunityTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setPropertyId(propIdSecondPart)
                .setEntityId(entityId).setIsOutgoing(false).setPropertyLabel("participates in").setStatements(List.of(
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


        var out2 = CommunityTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setPropertyId(9876543)
                .setEntityId(entityId).setIsOutgoing(true).setPropertyLabel("has fun with").setStatements(List.of(
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

        map.put(propIdFirstPart + "_in", in);

        map.put(propIdSecondPart + "_out", out);

        map.put(9876543 + "_out", out2);

        var entityTopStatements = CommunityEntityTopStatementsValue.newBuilder()
                .setEntityId(entityId).setProjectCount(1).setClassId(classId).setMap(map).build();

        var v = CommunityEntityTopStatementsWithConfigValue.newBuilder()
                .setLabelConfig(labelConfig)
                .setEntityTopStatements(entityTopStatements)
                .build();
        var result = CommunityEntityFulltext.createFulltext(v);
        assertThat(result).isEqualTo("foo\nparticipates in: Voyage 1, Voyage 2.\nhas friend: Max, Mia.\nhas fun with: Toy 1, Toy 2.");
    }

    @Test
    void testCreateFulltextMethodWithNullLabels() {
        var classId = 2;
        var entityId = "foo";
        var propIdFirstPart = 4;

        var map = new HashMap<String, CommunityTopStatementsWithPropLabelValue>();

        var in = CommunityTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setPropertyId(propIdFirstPart)
                .setEntityId(entityId).setIsOutgoing(false).setPropertyLabel("has friend").setStatements(List.of(
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(1f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setObjectLabel(null)
                                        .setSubjectLabel(null).build()).build(),
                        CommunityStatementValue.newBuilder().setStatementId(1)
                                .setAvgOrdNumOfDomain(2f)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setObjectLabel(null)
                                        .setSubjectLabel(null).build()).build()
                )).build();

        map.put(propIdFirstPart + "_in", in);


        var entityTopStatements = CommunityEntityTopStatementsValue.newBuilder()
                .setEntityId(entityId).setProjectCount(1).setClassId(classId).setMap(map).build();

        var v = CommunityEntityTopStatementsWithConfigValue.newBuilder()
                .setEntityTopStatements(entityTopStatements)
                .build();
        var result = CommunityEntityFulltext.createFulltext(v);
        assertThat(result).isEqualTo("");
    }

    @Test
    void testTopology() {
        var classId = 2;
        var entityId = "foo";
        var propIdSecondPart = 3;
        var propIdFirstPart = 4;
        var kC = OntomeClassKey.newBuilder().setClassId(classId).build();
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

        commnuityEntityLabelConfigTopic.pipeInput(kC, vC);

        var map = new HashMap<String, CommunityTopStatementsWithPropLabelValue>();

        var in = CommunityTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setPropertyId(propIdFirstPart)
                .setEntityId(entityId).setIsOutgoing(false).setPropertyLabel("has friend").setStatements(List.of(
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

        var out = CommunityTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setPropertyId(propIdSecondPart)
                .setEntityId(entityId).setIsOutgoing(true).setPropertyLabel("participates in").setStatements(List.of(
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


        var out2 = CommunityTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setPropertyId(9876543)
                .setEntityId(entityId).setIsOutgoing(true).setPropertyLabel("has fun with").setStatements(List.of(
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

        map.put(propIdFirstPart + "_in", in);

        map.put(propIdSecondPart + "_out", out);

        map.put(9876543 + "_out", out2);

        var entityTopStatements = CommunityEntityTopStatementsValue.newBuilder()
                .setEntityId(entityId).setProjectCount(1).setClassId(classId).setMap(map).build();


        var k = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        commnuityEntityTopStatementsTopic.pipeInput(k, entityTopStatements);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var record = outRecords.get(k);
        assertThat(record.getFulltext()).isEqualTo("foo\nparticipates in: Voyage 1, Voyage 2.\nhas friend: Max, Mia.\nhas fun with: Toy 1, Toy 2.");
    }

    @Test
    void testTopologyEntityWithoutLabelConfig() {
        var classId = 2;
        var entityId = "foo";
        var propIdSecondPart = 3;
        var propIdFirstPart = 4;

        var map = new HashMap<String, CommunityTopStatementsWithPropLabelValue>();

        var in = CommunityTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setPropertyId(propIdFirstPart)
                .setEntityId(entityId).setIsOutgoing(false).setPropertyLabel("has friend").setStatements(List.of(
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

        var out = CommunityTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setPropertyId(propIdSecondPart)
                .setEntityId(entityId).setIsOutgoing(true).setPropertyLabel("participates in").setStatements(List.of(
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


        var out2 = CommunityTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setPropertyId(9876543)
                .setEntityId(entityId).setIsOutgoing(true).setPropertyLabel("has fun with").setStatements(List.of(
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

        map.put(propIdFirstPart + "_in", in);

        map.put(propIdSecondPart + "_out", out);

        map.put(9876543 + "_out", out2);

        var entityTopStatements = CommunityEntityTopStatementsValue.newBuilder()
                .setEntityId(entityId).setProjectCount(1).setClassId(classId).setMap(map).build();


        var k = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        commnuityEntityTopStatementsTopic.pipeInput(k, entityTopStatements);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var record = outRecords.get(k);
        assertThat(record.getFulltext()).isEqualTo("foo\nhas fun with: Toy 1, Toy 2.\nhas friend: Max, Mia.\nparticipates in: Voyage 1, Voyage 2.");
    }


    @Test
    void testTopologyEntityWithoutStatements() {
        var classId = 2;
        var entityId = "foo";

        var map = new HashMap<String, CommunityTopStatementsWithPropLabelValue>();

        var entityTopStatements = CommunityEntityTopStatementsValue.newBuilder()
                .setEntityId(entityId).setProjectCount(1).setClassId(classId).setMap(map).build();


        var k = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        commnuityEntityTopStatementsTopic.pipeInput(k, entityTopStatements);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var record = outRecords.get(k);
        assertThat(record.getFulltext()).isEqualTo("");
    }

}
