package org.geovistory.toolbox.streams.fulltext.processors.org.geovistory.toolbox.streams.fulltext.processors.project;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.fulltext.Env;
import org.geovistory.toolbox.streams.fulltext.processors.project.ProjectEntityFulltext;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectEntityFulltextTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectEntityFulltextTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopStatementsTopic;
    private TestInputTopic<ProjectEntityKey, ProjectEntityLabelConfigValue> projectEntityWithLabelConfigTopic;
    private TestInputTopic<ProjectFieldLabelKey, ProjectFieldLabelValue> projectPropertyLabelTopic;
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

        projectTopStatementsTopic = testDriver.createInputTopic(
                Env.INSTANCE.TOPIC_PROJECT_TOP_STATEMENTS,
                avroSerdes.ProjectTopStatementsKey().serializer(),
                avroSerdes.ProjectTopStatementsValue().serializer());

        projectEntityWithLabelConfigTopic = testDriver.createInputTopic(
                Env.INSTANCE.TOPIC_PROJECT_ENTITY_WITH_LABEL_CONFIG,
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityLabelConfigValue().serializer());

        projectPropertyLabelTopic = testDriver.createInputTopic(
                Env.INSTANCE.TOPIC_PROJECT_PROPERTY_LABEL,
                avroSerdes.ProjectPropertyLabelKey().serializer(),
                avroSerdes.ProjectPropertyLabelValue().serializer());

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
    void testTopology() {
        var projectId = 1;
        var classId = 2;
        var entityId = "foo";
        var propIdSecondPart = 3;
        var propIdFirstPart = 4;
        var kC = ProjectEntityKey.newBuilder().setProjectId(projectId).setEntityId(entityId).build();
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

        projectEntityWithLabelConfigTopic.pipeInput(kC, vC);


        // Field 1

        var kL1 = ProjectFieldLabelKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setIsOutgoing(false)
                .setPropertyId(propIdFirstPart)
                .build();
        var vL1 = ProjectFieldLabelValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setIsOutgoing(false)
                .setPropertyId(propIdFirstPart)
                .setLanguageId(1)
                .setLabel("has friend")
                .build();
        projectPropertyLabelTopic.pipeInput(kL1, vL1);

        var k1 = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .setIsOutgoing(false)
                .setPropertyId(propIdFirstPart)
                .build();
        var v1 = ProjectTopStatementsValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(propIdFirstPart)
                .setEntityId(entityId).setIsOutgoing(false).setStatements(List.of(
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

        projectTopStatementsTopic.pipeInput(k1, v1);


        // Field 2

        var kL2 = ProjectFieldLabelKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setIsOutgoing(true)
                .setPropertyId(propIdSecondPart)
                .build();
        var vL2 = ProjectFieldLabelValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setIsOutgoing(true)
                .setPropertyId(propIdSecondPart)
                .setLanguageId(1)
                .setLabel("participates in")
                .build();
        projectPropertyLabelTopic.pipeInput(kL2, vL2);

        var k2 = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .setIsOutgoing(true)
                .setPropertyId(propIdSecondPart)
                .build();
        var v2 = ProjectTopStatementsValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(propIdSecondPart)
                .setEntityId(entityId).setIsOutgoing(true)
                .setStatements(List.of(
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
        projectTopStatementsTopic.pipeInput(k2, v2);

        // Field 3

        kL1 = ProjectFieldLabelKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setIsOutgoing(true)
                .setPropertyId(9876543)
                .build();
        vL1 = ProjectFieldLabelValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setIsOutgoing(true)
                .setPropertyId(9876543)
                .setLanguageId(1)
                .setLabel("has fun with")
                .build();
        projectPropertyLabelTopic.pipeInput(kL1, vL1);

        var k3 = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .setIsOutgoing(true)
                .setPropertyId(propIdSecondPart)
                .build();
        var v3 = ProjectTopStatementsValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(9876543)
                .setEntityId(entityId).setIsOutgoing(true)
                .setStatements(List.of(
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
        projectTopStatementsTopic.pipeInput(k3, v3);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var k = ProjectEntityKey.newBuilder().setProjectId(projectId).setEntityId(entityId).build();
        var record = outRecords.get(k);
        assertThat(record.getFulltext()).isEqualTo("has friend: Max, Mia.\nparticipates in: Voyage 1, Voyage 2.\nhas fun with: Toy 1, Toy 2.");
    }

    @Test
    void testTopologyEntityWithoutLabelConfig() {
        var projectId = 1;
        var classId = 2;
        var entityId = "foo";
        var propIdSecondPart = 3;
        var propIdFirstPart = 4;


        // Field 1

        var kL1 = ProjectFieldLabelKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setIsOutgoing(false)
                .setPropertyId(propIdFirstPart)
                .build();
        var vL1 = ProjectFieldLabelValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setIsOutgoing(false)
                .setPropertyId(propIdFirstPart)
                .setLanguageId(1)
                .setLabel("has friend")
                .build();
        projectPropertyLabelTopic.pipeInput(kL1, vL1);

        var k1 = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .setIsOutgoing(false)
                .setPropertyId(propIdFirstPart)
                .build();
        var v1 = ProjectTopStatementsValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(propIdFirstPart)
                .setEntityId(entityId).setIsOutgoing(false).setStatements(List.of(
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

        projectTopStatementsTopic.pipeInput(k1, v1);


        // Field 2

        var kL2 = ProjectFieldLabelKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setIsOutgoing(true)
                .setPropertyId(propIdSecondPart)
                .build();
        var vL2 = ProjectFieldLabelValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setIsOutgoing(false)
                .setPropertyId(propIdSecondPart)
                .setLanguageId(1)
                .setLabel("participates in")
                .build();
        projectPropertyLabelTopic.pipeInput(kL2, vL2);

        var k2 = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .setIsOutgoing(true)
                .setPropertyId(propIdSecondPart)
                .build();
        var v2 = ProjectTopStatementsValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(propIdSecondPart)
                .setEntityId(entityId).setIsOutgoing(true)
                .setStatements(List.of(
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
        projectTopStatementsTopic.pipeInput(k2, v2);

        // Field 3

        kL1 = ProjectFieldLabelKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setIsOutgoing(true)
                .setPropertyId(9876543)
                .build();
        vL1 = ProjectFieldLabelValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setIsOutgoing(false)
                .setPropertyId(9876543)
                .setLanguageId(1)
                .setLabel("has fun with")
                .build();
        projectPropertyLabelTopic.pipeInput(kL1, vL1);

        var k3 = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .setIsOutgoing(true)
                .setPropertyId(propIdSecondPart)
                .build();
        var v3 = ProjectTopStatementsValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(9876543)
                .setEntityId(entityId).setIsOutgoing(true)
                .setStatements(List.of(
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
        projectTopStatementsTopic.pipeInput(k3, v3);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var k = ProjectEntityKey.newBuilder().setProjectId(projectId).setEntityId(entityId).build();
        var record = outRecords.get(k);
        assertThat(record.getFulltext()).isEqualTo("participates in: Voyage 1, Voyage 2.\nhas friend: Max, Mia.\nhas fun with: Toy 1, Toy 2.");
    }


    @Test
    void testTopologyEntityWithoutStatements() {
        var projectId = 1;
        var classId = 2;
        var entityId = "foo";
        var propIdFirstPart = 4;


        // Field 1

        var kL1 = ProjectFieldLabelKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setIsOutgoing(false)
                .setPropertyId(propIdFirstPart)
                .build();
        var vL1 = ProjectFieldLabelValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .setIsOutgoing(false)
                .setPropertyId(propIdFirstPart)
                .setLanguageId(1)
                .setLabel("has friend")
                .build();
        projectPropertyLabelTopic.pipeInput(kL1, vL1);

        var k1 = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .setIsOutgoing(false)
                .setPropertyId(propIdFirstPart)
                .build();
        var v1 = ProjectTopStatementsValue.newBuilder()
                .setClassId(classId).setProjectId(projectId).setPropertyId(propIdFirstPart)
                .setEntityId(entityId).setIsOutgoing(false).setStatements(List.of()).build();

        projectTopStatementsTopic.pipeInput(k1, v1);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var k = ProjectEntityKey.newBuilder().setProjectId(projectId).setEntityId(entityId).build();
        var record = outRecords.get(k);
        assertThat(record.getFulltext()).isEqualTo("");
    }


}
