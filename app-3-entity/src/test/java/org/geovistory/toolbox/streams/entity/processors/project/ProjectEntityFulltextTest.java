package org.geovistory.toolbox.streams.entity.processors.project;


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

class ProjectEntityFulltextTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectEntityFulltextTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectEntityKey, ProjectEntityTopStatementsValue> projectEntityTopStatementsTopic;
    private TestInputTopic<ProjectClassKey, ProjectEntityLabelConfigValue> projectEntityLabelConfigTopic;

    private TestOutputTopic<ProjectEntityKey, ProjectEntityFulltextValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = ProjectEntityFulltext.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        projectEntityTopStatementsTopic = testDriver.createInputTopic(
                ProjectEntityFulltext.input.TOPICS.project_entity_top_statements,
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityTopStatementsValue().serializer());

        projectEntityLabelConfigTopic = testDriver.createInputTopic(
                ProjectEntityFulltext.input.TOPICS.project_entity_label_config_enriched,
                avroSerdes.ProjectClassKey().serializer(),
                avroSerdes.ProjectEntityLabelConfigValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                ProjectEntityFulltext.output.TOPICS.project_entity_fulltext,
                avroSerdes.ProjectEntityKey().deserializer(),
                avroSerdes.ProjectEntityFulltextValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testCreateFulltextMethod() {
        var projectId = 1;
        var classId = 2;
        var entityId = "foo";
        var propIdSecondPart = 3;
        var propIdFirstPart = 4;
        var labelConfig = ProjectEntityLabelConfigValue.newBuilder().setProjectId(projectId).setClassId(classId)
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

        var map = new HashMap<String, ProjectTopStatementsWithPropLabelValue>();

        var in = ProjectTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(propIdFirstPart)
                .setEntityId(entityId).setIsOutgoing(true).setPropertyLabel("has friend").setStatements(List.of(
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(1)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setObjectLabel("foo")
                                        .setSubjectLabel("Max").build()).build(),
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(2)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setObjectLabel("foo")
                                        .setSubjectLabel("Mia").build()).build()
                )).build();

        var out = ProjectTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(propIdSecondPart)
                .setEntityId(entityId).setIsOutgoing(false).setPropertyLabel("participates in").setStatements(List.of(
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(1)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdSecondPart)
                                        .setSubjectLabel("foo")
                                        .setObjectLabel("Voyage 1").build()).build(),
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(2)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdSecondPart)
                                        .setSubjectLabel("foo")
                                        .setObjectLabel("Voyage 2").build()).build()
                )).build();


        var out2 = ProjectTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(9876543)
                .setEntityId(entityId).setIsOutgoing(true).setPropertyLabel("has fun with").setStatements(List.of(
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(1)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(9876543)
                                        .setSubjectLabel("foo")
                                        .setObjectLabel("Toy 1").build()).build(),
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(2)
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

        var entityTopStatements = ProjectEntityTopStatementsValue.newBuilder()
                .setEntityId(entityId).setProjectId(projectId).setClassId(classId).setMap(map).build();

        var v = ProjectEntityTopStatementsWithConfigValue.newBuilder()
                .setLabelConfig(labelConfig)
                .setEntityTopStatements(entityTopStatements)
                .build();
        var result = ProjectEntityFulltext.createFulltext(v);
        assertThat(result).isEqualTo("foo\nparticipates in: Voyage 1, Voyage 2.\nhas friend: Max, Mia.\nhas fun with: Toy 1, Toy 2.");
    }

    @Test
    void testCreateFulltextMethodWithNullLabels() {
        var projectId = 1;
        var classId = 2;
        var entityId = "foo";
        var propIdFirstPart = 4;

        var map = new HashMap<String, ProjectTopStatementsWithPropLabelValue>();

        var in = ProjectTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(propIdFirstPart)
                .setEntityId(entityId).setIsOutgoing(false).setPropertyLabel("has friend").setStatements(List.of(
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(1)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setObjectLabel(null)
                                        .setSubjectLabel(null).build()).build(),
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(2)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setObjectLabel(null)
                                        .setSubjectLabel(null).build()).build()
                )).build();

        map.put(propIdFirstPart + "_in", in);


        var entityTopStatements = ProjectEntityTopStatementsValue.newBuilder()
                .setEntityId(entityId).setProjectId(projectId).setClassId(classId).setMap(map).build();

        var v = ProjectEntityTopStatementsWithConfigValue.newBuilder()
                .setEntityTopStatements(entityTopStatements)
                .build();
        var result = ProjectEntityFulltext.createFulltext(v);
        assertThat(result).isEqualTo("");
    }

    @Test
    void testTopology() {
        var projectId = 1;
        var classId = 2;
        var entityId = "foo";
        var propIdSecondPart = 3;
        var propIdFirstPart = 4;
        var kC = ProjectClassKey.newBuilder().setProjectId(projectId).setClassId(classId).build();
        var vC = ProjectEntityLabelConfigValue.newBuilder().setProjectId(projectId).setClassId(classId)
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

        projectEntityLabelConfigTopic.pipeInput(kC, vC);

        var map = new HashMap<String, ProjectTopStatementsWithPropLabelValue>();

        var in = ProjectTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(propIdFirstPart)
                .setEntityId(entityId).setIsOutgoing(false).setPropertyLabel("has friend").setStatements(List.of(
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(1)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setObjectLabel("foo")
                                        .setSubjectLabel("Max").build()).build(),
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(2)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setObjectLabel("foo")
                                        .setSubjectLabel("Mia").build()).build()
                )).build();

        var out = ProjectTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(propIdSecondPart)
                .setEntityId(entityId).setIsOutgoing(true).setPropertyLabel("participates in").setStatements(List.of(
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(1)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdSecondPart)
                                        .setSubjectLabel("foo")
                                        .setObjectLabel("Voyage 1").build()).build(),
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(2)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdSecondPart)
                                        .setSubjectLabel("foo")
                                        .setObjectLabel("Voyage 2").build()).build()
                )).build();


        var out2 = ProjectTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(9876543)
                .setEntityId(entityId).setIsOutgoing(true).setPropertyLabel("has fun with").setStatements(List.of(
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(1)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(9876543)
                                        .setSubjectLabel("foo")
                                        .setObjectLabel("Toy 1").build()).build(),
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(2)
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

        var entityTopStatements = ProjectEntityTopStatementsValue.newBuilder()
                .setEntityId(entityId).setProjectId(projectId).setClassId(classId).setMap(map).build();


        var k = ProjectEntityKey.newBuilder().setProjectId(projectId).setEntityId(entityId).build();
        projectEntityTopStatementsTopic.pipeInput(k, entityTopStatements);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var record = outRecords.get(k);
        assertThat(record.getFulltext()).isEqualTo("foo\nparticipates in: Voyage 1, Voyage 2.\nhas friend: Max, Mia.\nhas fun with: Toy 1, Toy 2.");
    }

    @Test
    void testTopologyEntityWithoutLabelConfig() {
        var projectId = 1;
        var classId = 2;
        var entityId = "foo";
        var propIdSecondPart = 3;
        var propIdFirstPart = 4;

        var map = new HashMap<String, ProjectTopStatementsWithPropLabelValue>();

        var in = ProjectTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(propIdFirstPart)
                .setEntityId(entityId).setIsOutgoing(false).setPropertyLabel("has friend").setStatements(List.of(
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(1)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setObjectLabel("foo")
                                        .setSubjectLabel("Max").build()).build(),
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(2)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setObjectLabel("foo")
                                        .setSubjectLabel("Mia").build()).build()
                )).build();

        var out = ProjectTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(propIdSecondPart)
                .setEntityId(entityId).setIsOutgoing(true).setPropertyLabel("participates in").setStatements(List.of(
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(1)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdSecondPart)
                                        .setSubjectLabel("foo")
                                        .setObjectLabel("Voyage 1").build()).build(),
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(2)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdSecondPart)
                                        .setSubjectLabel("foo")
                                        .setObjectLabel("Voyage 2").build()).build()
                )).build();


        var out2 = ProjectTopStatementsWithPropLabelValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(9876543)
                .setEntityId(entityId).setIsOutgoing(true).setPropertyLabel("has fun with").setStatements(List.of(
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(1)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(9876543)
                                        .setSubjectLabel("foo")
                                        .setObjectLabel("Toy 1").build()).build(),
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(2)
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

        var entityTopStatements = ProjectEntityTopStatementsValue.newBuilder()
                .setEntityId(entityId).setProjectId(projectId).setClassId(classId).setMap(map).build();


        var k = ProjectEntityKey.newBuilder().setProjectId(projectId).setEntityId(entityId).build();
        projectEntityTopStatementsTopic.pipeInput(k, entityTopStatements);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var record = outRecords.get(k);
        assertThat(record.getFulltext()).isEqualTo("foo\nhas fun with: Toy 1, Toy 2.\nhas friend: Max, Mia.\nparticipates in: Voyage 1, Voyage 2.");
    }


    @Test
    void testTopologyEntityWithoutStatements() {
        var projectId = 1;
        var classId = 2;
        var entityId = "foo";

        var map = new HashMap<String, ProjectTopStatementsWithPropLabelValue>();

        var entityTopStatements = ProjectEntityTopStatementsValue.newBuilder()
                .setEntityId(entityId).setProjectId(projectId).setClassId(classId).setMap(map).build();


        var k = ProjectEntityKey.newBuilder().setProjectId(projectId).setEntityId(entityId).build();
        projectEntityTopStatementsTopic.pipeInput(k, entityTopStatements);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var record = outRecords.get(k);
        assertThat(record.getFulltext()).isEqualTo("");
    }

}
