package org.geovistory.toolbox.streams.topology;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.app.RegisterInputTopic;
import org.geovistory.toolbox.streams.app.RegisterOutputTopic;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.topologies.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectTopStatementsRecursionTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectTopStatementsRecursionTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;

    // input topics of ProjectStatement
    private TestInputTopic<dev.information.statement.Key, StatementEnrichedValue> statementWitEntityTopic;
    private TestInputTopic<dev.information.statement.Key, StatementEnrichedValue> statementWithLiteralTopic;    private TestInputTopic<dev.projects.info_proj_rel.Key, dev.projects.info_proj_rel.Value> proInfoProjRelTopic;

    // input topics of ProjectTopOutgoingStatements
    // -> output of Topology ProjectStatement

    // input topics of ProjectTopIncomingStatements
    // -> output of Topology ProjectStatement

    // input topics of ProjectTopStatements
    // -> output of Topology ProjectTopIncomingStatements & ProjectTopOutgoingStatements

    // input topics of ProjectEntityLabel
    private TestInputTopic<ProjectClassKey, ProjectEntityLabelConfigValue> projectEntityLabelConfigTopic;
    private TestInputTopic<ProjectEntityKey, ProjectEntityValue> projectEntityTopic;
    //private TestInputTopic<ProjectEntityKey, ProjectEntityLabelValue> inputProjectEntityLabelTopic;

    private TestOutputTopic<ProjectTopStatementsKey, ProjectTopStatementsValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        StreamsBuilder builder = new StreamsBuilder();
        var inputTopics = new RegisterInputTopic(builder);
        var outputTopics = new RegisterOutputTopic(builder);

        var proInfoProjRelTable = inputTopics.proInfoProjRelTable();
        var projectEntityLabelTable = outputTopics.projectEntityLabelTable();
        var statementWithLiteralTable = outputTopics.statementWithLiteralTable();
        var statementWithEntityTable = outputTopics.statementWithEntityTable();
        var projectStatementWithEntityTable = outputTopics.projectStatementWithEntityTable();
        var projectEntityTable = outputTopics.projectEntityTable();
        var projectEntityLabelConfigTable = outputTopics.projectEntityLabelConfigTable();

        // add sub-topology ProjectStatement
        ProjectStatementWithEntity.addProcessors(builder,
                statementWithEntityTable,
                proInfoProjRelTable
        );
        // add sub-topology ProjectStatement
        var projectStatementWithLiteral = ProjectStatementWithLiteral.addProcessors(builder,
                statementWithLiteralTable,
                proInfoProjRelTable
        );
        // add sub-topology ProjectTopIncomingStatements
        var projectTopIncomingStatements = ProjectTopIncomingStatements.addProcessors(builder,
                projectStatementWithEntityTable,
                projectEntityLabelTable
        );

        // add sub-topology ProjectTopOutgoingStatements
        var projectTopOutgoingStatements = ProjectTopOutgoingStatements.addProcessors(builder,
                projectStatementWithLiteral.ProjectStatementStream(),
                projectStatementWithEntityTable,
                projectEntityLabelTable
        );

        // add sub-topology ProjectTopStatements
        var projectTopStatements = ProjectTopStatements.addProcessors(builder,
                projectTopOutgoingStatements.projectTopStatementStream(),
                projectTopIncomingStatements.projectTopStatementStream()
        );

        // Add processors for ProjectEntityLabel
        ProjectEntityLabel.addProcessors(builder,
                projectEntityTable,
                projectEntityLabelConfigTable,
                projectTopStatements.projectTopStatementTable()
        );


        Topology topology = builder.build();

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        projectEntityLabelConfigTopic = testDriver.createInputTopic(
                ProjectEntityLabel.input.TOPICS.project_entity_label_config_enriched,
                avroSerdes.ProjectClassKey().serializer(),
                avroSerdes.ProjectEntityLabelConfigValue().serializer());

        statementWitEntityTopic = testDriver.createInputTopic(
                StatementEnriched.output.TOPICS.statement_with_entity,
                avroSerdes.InfStatementKey().serializer(),
                avroSerdes.StatementEnrichedValue().serializer());

        statementWithLiteralTopic = testDriver.createInputTopic(
                StatementEnriched.output.TOPICS.statement_with_literal,
                avroSerdes.InfStatementKey().serializer(),
                avroSerdes.StatementEnrichedValue().serializer());

        proInfoProjRelTopic = testDriver.createInputTopic(
                ProjectStatementWithEntity.input.TOPICS.pro_info_proj_rel,
                avroSerdes.ProInfoProjRelKey().serializer(),
                avroSerdes.ProInfoProjRelValue().serializer());


        projectEntityTopic = testDriver.createInputTopic(
                ProjectEntityLabel.input.TOPICS.project_entity,
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                ProjectTopStatements.output.TOPICS.project_top_statements,
                avroSerdes.ProjectTopStatementsKey().deserializer(),
                avroSerdes.ProjectTopStatementsValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }


    /**
     * Test number of output records:
     * For the three entities, one person and two names, it should create four records:
     * 1. person: entity_label = ""
     * 2. name1: entity_label = "Name 1"
     * 3. name2: entity_label = "Name 2"
     * 4. person: entity_label = "Name 1"
     */
    @Test
    void testProjectTopStatementsCountRecords() {
        var projectId = 10;
        var statementOneId = 20;
        var statementTwoId = 21;
        var statementThreeId = 22;
        var statementFourId = 23;
        var propertyId = 30;
        var propertyHasStringId = 31;
        var classPersonId = 3; // person
        var classNameId = 4; // name
        var entityPerson = "person";
        var entityName1 = "name1";
        var entityName2 = "name2";

        // add statement one
        var kS = dev.information.statement.Key.newBuilder().setPkEntity(statementOneId).build();
        var vS = StatementEnrichedValue.newBuilder().setSubjectId(entityPerson).setPropertyId(propertyId).setObjectId(entityName1).build();
        statementWitEntityTopic.pipeInput(kS, vS);

        // add relation between project and statement one
        var kR = dev.projects.info_proj_rel.Key.newBuilder().setFkEntity(statementOneId).setFkProject(projectId).build();
        var vR = dev.projects.info_proj_rel.Value.newBuilder().setSchemaName("").setTableName("").setEntityVersion(1)
                .setTmspLastModification("2021-01-01T12:59:50.716896Z")
                .setFkEntity(statementOneId).setFkProject(projectId).setIsInProject(true).build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add statement two
        kS = dev.information.statement.Key.newBuilder().setPkEntity(statementTwoId).build();
        vS = StatementEnrichedValue.newBuilder().setSubjectId(entityPerson).setPropertyId(propertyId).setObjectId(entityName2)
                .build();
        statementWitEntityTopic.pipeInput(kS, vS);

        // add relation between project and statement two
        kR = dev.projects.info_proj_rel.Key.newBuilder().setFkEntity(statementTwoId).setFkProject(projectId).build();
        vR = dev.projects.info_proj_rel.Value.newBuilder().setSchemaName("").setTableName("").setEntityVersion(1)
                .setTmspLastModification("2020-01-01T12:59:50.716896Z")
                .setFkEntity(statementTwoId).setFkProject(projectId).setIsInProject(true).build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add statement three
        kS = dev.information.statement.Key.newBuilder().setPkEntity(statementThreeId).build();
        vS = StatementEnrichedValue.newBuilder().setSubjectId(entityName1).setPropertyId(propertyHasStringId)
                .setObjectLabel("Name 1")
                .setObjectLiteral(ObjectValue.newBuilder().setLabel("Name 1").setId("").setClassId(0).build()).build();
        statementWithLiteralTopic.pipeInput(kS, vS);

        // add relation between project and statement three
        kR = dev.projects.info_proj_rel.Key.newBuilder().setFkEntity(statementThreeId).setFkProject(projectId).build();
        vR = dev.projects.info_proj_rel.Value.newBuilder().setSchemaName("").setTableName("").setEntityVersion(1)
                .setFkEntity(statementThreeId).setFkProject(projectId).setIsInProject(true).build();
        proInfoProjRelTopic.pipeInput(kR, vR);


        // add statement four
        kS = dev.information.statement.Key.newBuilder().setPkEntity(statementFourId).build();
        vS = StatementEnrichedValue.newBuilder().setSubjectId(entityName2).setPropertyId(propertyHasStringId)
                .setObjectLabel("Name 2")
                .setObjectLiteral(ObjectValue.newBuilder().setLabel("Name 2").setId("").setClassId(0).build()).build();
        statementWithLiteralTopic.pipeInput(kS, vS);

        // add relation between project and statement four
        kR = dev.projects.info_proj_rel.Key.newBuilder().setFkEntity(statementFourId).setFkProject(projectId).build();
        vR = dev.projects.info_proj_rel.Value.newBuilder().setSchemaName("").setTableName("").setEntityVersion(1)
                .setFkEntity(statementFourId).setFkProject(projectId).setIsInProject(true).build();
        proInfoProjRelTopic.pipeInput(kR, vR);


        // add project entity foo
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityPerson).setProjectId(projectId).build();
        var vE = ProjectEntityValue.newBuilder().setEntityId(entityPerson).setProjectId(projectId).setClassId(classPersonId).build();
        projectEntityTopic.pipeInput(kE, vE);

        // add project entity one
        kE = ProjectEntityKey.newBuilder().setEntityId(entityName1).setProjectId(projectId).build();
        vE = ProjectEntityValue.newBuilder().setEntityId(entityName1).setProjectId(projectId).setClassId(classNameId).build();
        projectEntityTopic.pipeInput(kE, vE);

        // add project entity two
        kE = ProjectEntityKey.newBuilder().setEntityId(entityName2).setProjectId(projectId).build();
        vE = ProjectEntityValue.newBuilder().setEntityId(entityName2).setProjectId(projectId).setClassId(classNameId).build();
        projectEntityTopic.pipeInput(kE, vE);


        // Add entity label configuration for person
        var kC = ProjectClassKey.newBuilder().setProjectId(projectId).setClassId(classPersonId).build();
        var vC = ProjectEntityLabelConfigValue.newBuilder().setProjectId(projectId).setClassId(classPersonId)
                .setConfig(EntityLabelConfig.newBuilder().setLabelParts(List.of(
                        EntityLabelConfigPart.newBuilder().setOrdNum(1).setField(EntityLabelConfigPartField.newBuilder()
                                .setFkProperty(propertyId)
                                .setIsOutgoing(true)
                                .setNrOfStatementsInLabel(1).build()).build()
                )).build()).build();
        projectEntityLabelConfigTopic.pipeInput(kC, vC);

        // Add entity label configuration for name
        kC = ProjectClassKey.newBuilder().setProjectId(projectId).setClassId(classNameId).build();
        vC = ProjectEntityLabelConfigValue.newBuilder().setProjectId(projectId).setClassId(classNameId)
                .setConfig(EntityLabelConfig.newBuilder().setLabelParts(List.of(
                        EntityLabelConfigPart.newBuilder().setOrdNum(1).setField(EntityLabelConfigPartField.newBuilder()
                                .setFkProperty(propertyHasStringId)
                                .setIsOutgoing(true)
                                .setNrOfStatementsInLabel(1).build()).build()
                )).build()).build();
        projectEntityLabelConfigTopic.pipeInput(kC, vC);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readRecordsToList();
        System.out.println("Number of records: " + outRecords.size());

        for (var r : outRecords) {
            var k = r.key();
            var v = r.value();
            System.out.println();

            var s = new ArrayList<String>();
            s.add(k.getEntityId());
            s.add(k.getIsOutgoing() + "");
            var statements = v.getStatements();
            s.add("statements-count: " + v.getStatements().size());
            System.out.println(">  " + String.join("\t", s));

            for (var statement : statements) {
                var s2 = new ArrayList<String>();

                s2.add("StatementId: " + statement.getStatementId());
                s2.add("SubjectLabel: " + statement.getStatement().getSubjectLabel());
                s2.add("ObjectLabel: " + statement.getStatement().getObjectLabel());
                System.out.println("   " + String.join("\t", s2));
            }


        }
        assertThat(outRecords).hasSize(14);

    }

}
