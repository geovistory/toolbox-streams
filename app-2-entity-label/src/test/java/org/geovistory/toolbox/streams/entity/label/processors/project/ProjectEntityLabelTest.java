package org.geovistory.toolbox.streams.entity.label.processors.project;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label.processsors.project.ProjectEntityLabel;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectEntityLabelTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectEntityLabelTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectClassKey, ProjectEntityLabelConfigValue> projectEntityLabelConfigTopic;
    private TestInputTopic<ProjectEntityKey, ProjectEntityValue> projectEntityTopic;
    private TestInputTopic<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopStatements;
    private TestOutputTopic<ProjectEntityKey, ProjectEntityLabelValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = ProjectEntityLabel.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        projectEntityLabelConfigTopic = testDriver.createInputTopic(
                ProjectEntityLabel.input.TOPICS.project_entity_label_config,
                avroSerdes.ProjectClassKey().serializer(),
                avroSerdes.ProjectEntityLabelConfigValue().serializer());

        projectTopStatements = testDriver.createInputTopic(
                ProjectEntityLabel.input.TOPICS.project_top_statements,
                avroSerdes.ProjectTopStatementsKey().serializer(),
                avroSerdes.ProjectTopStatementsValue().serializer());

        projectEntityTopic = testDriver.createInputTopic(
                ProjectEntityLabel.input.TOPICS.project_entity,
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                ProjectEntityLabel.output.TOPICS.project_entity_label,
                avroSerdes.ProjectEntityKey().deserializer(),
                avroSerdes.ProjectEntityLabelValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }


    /**
     * The goal of this test is to compose the following entity_label: "S1, S2, S3".
     */
    @Test
    void testProjectEntityLabel() {

        var entityId = "i1";
        var projectId = 2;
        var classId = 3;

        var propIdFirstPart = 5;
        var propIdSecondPart = 4;

        var expected = "S1, S2, S3";

        // add an entity
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(projectId).build();
        var vE = ProjectEntityValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setClassId(3).build();
        projectEntityTopic.pipeInput(kE, vE);
        /*
         * The entity label configuration is:
         * - First Part: property_it = 5, is_outgoing = false, number_of_statements = 1
         * - Second Part: property_it = 4, is_outgoing = true, number_of_statements = 2
         */
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
        /*
         * Statements for the first part:
         * - subject_id = i1, property_id = 5, object_id = i1, ord_num_for_domain = 1, subject_label = S1
         * - subject_id = i1, property_id = 5, object_id = i1, ord_num_for_domain = 2, subject_label = NOISE
         */
        var kS = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId).setEntityId(entityId)
                .setPropertyId(propIdFirstPart).setIsOutgoing(false).build();
        var vS = ProjectTopStatementsValue.newBuilder()
                .setProjectId(projectId).setEntityId(entityId)
                .setPropertyId(propIdFirstPart).setIsOutgoing(false)
                .setStatements(List.of(
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(1)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setSubjectLabel("S1").build()).build(),
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(2)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setSubjectLabel("NOISE").build()).build()
                )).build();
        projectTopStatements.pipeInput(kS, vS);

        /*
         * Statements for the Second part:
         * - subject_id = i1, property_id = 4, object_id = i1, ord_num_for_range = 1, object_label = S2
         * - subject_id = i1, property_id = 4, object_id = i1, ord_num_for_range = 2, object_label = S3
         * - subject_id = i1, property_id = 4, object_id = i1, ord_num_for_range = 3, object_label = NOISE
         */
        kS = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId).setEntityId(entityId)
                .setPropertyId(propIdSecondPart).setIsOutgoing(true).build();
        vS = ProjectTopStatementsValue.newBuilder()
                .setProjectId(projectId).setEntityId(entityId)
                .setPropertyId(propIdSecondPart).setIsOutgoing(true)
                .setStatements(List.of(
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfRange(1)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdSecondPart)
                                        .setObjectLabel("S2").build()).build(),
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfRange(2)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdSecondPart)
                                        .setObjectLabel("S3").build()).build(),
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfRange(3)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdSecondPart)
                                        .setObjectLabel("NOISE").build()).build()
                )).build();
        projectTopStatements.pipeInput(kS, vS);

        /*
         * Add the same statements again to see if it still behaves correctly
         */
        projectTopStatements.pipeInput(kS, vS);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectEntityKey.newBuilder()
                .setEntityId(entityId)
                .setProjectId(projectId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getLabel()).isEqualTo(expected);

    }

    @Test
    void testShouldAllowNrOfStatementsInLabelBiggerThanStatements() {

        var entityId = "i1";
        var projectId = 2;
        var classId = 3;

        var propIdFirstPart = 5;

        var expected = "S1";

        // add an entity
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(projectId).build();
        var vE = ProjectEntityValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setClassId(3).build();
        projectEntityTopic.pipeInput(kE, vE);
        /*
         * The entity label configuration is:
         * - property_it = 5, is_outgoing = false, number_of_statements = 3
         */
        var kC = ProjectClassKey.newBuilder().setProjectId(projectId).setClassId(classId).build();
        var vC = ProjectEntityLabelConfigValue.newBuilder().setProjectId(projectId).setClassId(classId)
                .setConfig(EntityLabelConfig.newBuilder().setLabelParts(List.of(
                        EntityLabelConfigPart.newBuilder().setOrdNum(1).setField(EntityLabelConfigPartField.newBuilder()
                                .setFkProperty(propIdFirstPart)
                                .setIsOutgoing(false)
                                .setNrOfStatementsInLabel(3).build()).build()
                )).build()).build();

        projectEntityLabelConfigTopic.pipeInput(kC, vC);
        /*
         * Statements for the first part:
         * - subject_id = i1, property_id = 5, object_id = i1, ord_num_for_domain = 1, subject_label = S1
         */
        var kS = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId).setEntityId(entityId)
                .setPropertyId(propIdFirstPart).setIsOutgoing(false).build();
        var vS = ProjectTopStatementsValue.newBuilder()
                .setProjectId(projectId).setEntityId(entityId)
                .setPropertyId(propIdFirstPart).setIsOutgoing(false)
                .setStatements(List.of(
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(1)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setObjectId(entityId)
                                        .setPropertyId(propIdFirstPart)
                                        .setSubjectLabel("S1").build()).build()
                )).build();
        projectTopStatements.pipeInput(kS, vS);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectEntityKey.newBuilder()
                .setEntityId(entityId)
                .setProjectId(projectId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getLabel()).isEqualTo(expected);

    }



}
