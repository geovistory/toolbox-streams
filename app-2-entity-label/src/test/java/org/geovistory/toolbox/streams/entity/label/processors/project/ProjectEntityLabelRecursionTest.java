package org.geovistory.toolbox.streams.entity.label.processors.project;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label.*;
import org.geovistory.toolbox.streams.entity.label.processsors.project.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This class tests if the following self-joining sub-topologies do not create an infinite loop:
 * - ProjectStatement --depends-on--> ProjectEntityLabel --depends-on-->
 * ProjectTopStatements --depends-on--> ProjectTopOutgoingStatements --depends-on--> ProjectStatement
 * --depends-on--> ProjectTopIncomingStatements --depends-on--> ProjectStatement
 */
class ProjectEntityLabelRecursionTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectEntityLabelRecursionTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;

    // input topics of ProjectStatement
    private TestInputTopic<ts.information.statement.Key, StatementEnrichedValue> statementWitEntityTopic;
    private TestInputTopic<ts.information.statement.Key, StatementEnrichedValue> statementWithLiteralTopic;
    private TestInputTopic<ts.projects.info_proj_rel.Key, ts.projects.info_proj_rel.Value> proInfoProjRelTopic;

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

    private TestOutputTopic<ProjectEntityKey, ProjectEntityLabelValue> outputTopic;

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


        var proInfoProjRelTable = registerInputTopic.proInfoProjRelTable();
        var projectEntityLabelTable = registerInnerTopic.projectEntityLabelTable();
        var communityToolboxEntityLabelTable = registerInnerTopic.communityToolboxEntityLabelTable();
        var statementWithLiteralTable = registerInputTopic.statementWithLiteralTable();
        var statementWithEntityTable = registerInputTopic.statementWithEntityTable();
        var projectStatementWithEntityTable = registerInnerTopic.projectStatementWithEntityTable();
        var projectEntityTable = registerInnerTopic.projectEntityTable();
        var projectEntityLabelConfigTable = registerInputTopic.projectEntityLabelConfigTable();

        // add sub-topology ProjectStatement
        var projectStatementWithEntity = new ProjectStatementWithEntity(avroSerdes, registerInputTopic, registerInnerTopic, outputTopicNames);
        projectStatementWithEntity.addProcessors(
                statementWithEntityTable,
                proInfoProjRelTable
        );
        // add sub-topology ProjectStatement
        var projectStatementWithLiteral = new ProjectStatementWithLiteral(avroSerdes, registerInputTopic, registerInnerTopic, outputTopicNames);
        var projectStatementWithLiteralReturn = projectStatementWithLiteral.addProcessors(
                statementWithLiteralTable,
                proInfoProjRelTable
        );
        // add sub-topology ProjectTopIncomingStatements
        var projectTopIncomingStatements = new ProjectTopIncomingStatements(avroSerdes, registerInputTopic, registerInnerTopic, outputTopicNames);
        var projectTopIncomingStatementsReturn = projectTopIncomingStatements.addProcessors(
                projectStatementWithEntityTable,
                projectEntityLabelTable,
                communityToolboxEntityLabelTable
        );

        // add sub-topology ProjectTopOutgoingStatements
        var projectTopOutgoingStatements = new ProjectTopOutgoingStatements(avroSerdes, registerInputTopic, registerInnerTopic, outputTopicNames);
        var projectTopOutgoingStatementsReturn = projectTopOutgoingStatements.addProcessors(
                projectStatementWithLiteralReturn.ProjectStatementStream(),
                projectStatementWithEntityTable,
                projectEntityLabelTable,
                communityToolboxEntityLabelTable
        );

        // add sub-topology ProjectTopStatements
        var projectTopStatements = new ProjectTopStatements(avroSerdes, registerInputTopic, registerInnerTopic, outputTopicNames);
        var projectTopStatementsReturn = projectTopStatements.addProcessors(
                projectTopOutgoingStatementsReturn.projectTopStatementStream(),
                projectTopIncomingStatementsReturn.projectTopStatementStream()
        );

        // Add processors for ProjectEntityLabel
        var projectEntityLabel = new ProjectEntityLabel(avroSerdes, registerInputTopic, registerInnerTopic, outputTopicNames);
        projectEntityLabel.addProcessors(
                projectEntityTable,
                projectEntityLabelConfigTable,
                projectTopStatementsReturn.projectTopStatementTable()
        );


        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);


        projectEntityLabelConfigTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectEntityLabelConfig(),
                avroSerdes.ProjectClassKey().serializer(),
                avroSerdes.ProjectEntityLabelConfigValue().serializer());

        statementWitEntityTopic = testDriver.createInputTopic(
                inputTopicNames.getStatementWithEntity(),
                avroSerdes.InfStatementKey().serializer(),
                avroSerdes.StatementEnrichedValue().serializer());

        statementWithLiteralTopic = testDriver.createInputTopic(
                inputTopicNames.getStatementWithLiteral(),
                avroSerdes.InfStatementKey().serializer(),
                avroSerdes.StatementEnrichedValue().serializer());

        proInfoProjRelTopic = testDriver.createInputTopic(
                inputTopicNames.proInfoProjRel(),
                avroSerdes.ProInfoProjRelKey().serializer(),
                avroSerdes.ProInfoProjRelValue().serializer());


        projectEntityTopic = testDriver.createInputTopic(
                outputTopicNames.projectEntity(),
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectEntityLabel(),
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
        var kS = ts.information.statement.Key.newBuilder().setPkEntity(statementOneId).build();
        var vS = StatementEnrichedValue.newBuilder().setSubjectId(entityPerson).setPropertyId(propertyId).setObjectId(entityName1)
                .build();
        statementWitEntityTopic.pipeInput(kS, vS);

        // add relation between project and statement one
        var kR = ts.projects.info_proj_rel.Key.newBuilder().setFkEntity(statementOneId).setFkProject(projectId).build();
        var vR = ts.projects.info_proj_rel.Value.newBuilder().setSchemaName("").setTableName("").setEntityVersion(1)
                .setTmspLastModification("2021-01-01T12:59:50.716896Z")
                .setFkEntity(statementOneId).setFkProject(projectId).setIsInProject(true).build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add statement two
        kS = ts.information.statement.Key.newBuilder().setPkEntity(statementTwoId).build();
        vS = StatementEnrichedValue.newBuilder().setSubjectId(entityPerson).setPropertyId(propertyId).setObjectId(entityName2)
                .build();
        statementWitEntityTopic.pipeInput(kS, vS);

        // add relation between project and statement two
        kR = ts.projects.info_proj_rel.Key.newBuilder().setFkEntity(statementTwoId).setFkProject(projectId).build();
        vR = ts.projects.info_proj_rel.Value.newBuilder().setSchemaName("").setTableName("").setEntityVersion(1)
                .setTmspLastModification("2020-01-01T12:59:50.716896Z")
                .setFkEntity(statementTwoId).setFkProject(projectId).setIsInProject(true).build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add statement three
        kS = ts.information.statement.Key.newBuilder().setPkEntity(statementThreeId).build();
        vS = StatementEnrichedValue.newBuilder().setSubjectId(entityName1).setPropertyId(propertyHasStringId)
                .setObjectLabel("Name 1")
                .setObject(NodeValue.newBuilder().setLabel("Name 1").setId("").setClassId(0).build()).build();
        statementWithLiteralTopic.pipeInput(kS, vS);

        // add relation between project and statement three
        kR = ts.projects.info_proj_rel.Key.newBuilder().setFkEntity(statementThreeId).setFkProject(projectId).build();
        vR = ts.projects.info_proj_rel.Value.newBuilder().setSchemaName("").setTableName("").setEntityVersion(1)
                .setFkEntity(statementThreeId).setFkProject(projectId).setIsInProject(true).build();
        proInfoProjRelTopic.pipeInput(kR, vR);


        // add statement four
        kS = ts.information.statement.Key.newBuilder().setPkEntity(statementFourId).build();
        vS = StatementEnrichedValue.newBuilder().setSubjectId(entityName2).setPropertyId(propertyHasStringId)
                .setObjectLabel("Name 2")
                .setObject(NodeValue.newBuilder().setLabel("Name 2").setId("").setClassId(0).build()).build();
        statementWithLiteralTopic.pipeInput(kS, vS);

        // add relation between project and statement four
        kR = ts.projects.info_proj_rel.Key.newBuilder().setFkEntity(statementFourId).setFkProject(projectId).build();
        vR = ts.projects.info_proj_rel.Value.newBuilder().setSchemaName("").setTableName("").setEntityVersion(1)
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
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(3);

        var resultingKey = ProjectEntityKey.newBuilder().setEntityId(entityName2).setProjectId(projectId).build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getLabel()).isEqualTo("Name 2");

        resultingKey = ProjectEntityKey.newBuilder().setEntityId(entityPerson).setProjectId(projectId).build();
        record = outRecords.get(resultingKey);
        assertThat(record.getLabel()).isEqualTo("Name 1");

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
    void testProjectEntityLabelCountRecords() {
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
        var kS = ts.information.statement.Key.newBuilder().setPkEntity(statementOneId).build();
        var vS = StatementEnrichedValue.newBuilder().setSubjectId(entityPerson).setPropertyId(propertyId).setObjectId(entityName1).build();
        statementWitEntityTopic.pipeInput(kS, vS);

        // add relation between project and statement one
        var kR = ts.projects.info_proj_rel.Key.newBuilder().setFkEntity(statementOneId).setFkProject(projectId).build();
        var vR = ts.projects.info_proj_rel.Value.newBuilder().setSchemaName("").setTableName("").setEntityVersion(1)
                .setTmspLastModification("2021-01-01T12:59:50.716896Z")
                .setFkEntity(statementOneId).setFkProject(projectId).setIsInProject(true).build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add statement two
        kS = ts.information.statement.Key.newBuilder().setPkEntity(statementTwoId).build();
        vS = StatementEnrichedValue.newBuilder().setSubjectId(entityPerson).setPropertyId(propertyId).setObjectId(entityName2)
                .build();
        statementWitEntityTopic.pipeInput(kS, vS);

        // add relation between project and statement two
        kR = ts.projects.info_proj_rel.Key.newBuilder().setFkEntity(statementTwoId).setFkProject(projectId).build();
        vR = ts.projects.info_proj_rel.Value.newBuilder().setSchemaName("").setTableName("").setEntityVersion(1)
                .setTmspLastModification("2020-01-01T12:59:50.716896Z")
                .setFkEntity(statementTwoId).setFkProject(projectId).setIsInProject(true).build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add statement three
        kS = ts.information.statement.Key.newBuilder().setPkEntity(statementThreeId).build();
        vS = StatementEnrichedValue.newBuilder().setSubjectId(entityName1).setPropertyId(propertyHasStringId)
                .setObjectLabel("Name 1")
                .setObject(NodeValue.newBuilder().setLabel("Name 1").setId("").setClassId(0).build()).build();
        statementWithLiteralTopic.pipeInput(kS, vS);

        // add relation between project and statement three
        kR = ts.projects.info_proj_rel.Key.newBuilder().setFkEntity(statementThreeId).setFkProject(projectId).build();
        vR = ts.projects.info_proj_rel.Value.newBuilder().setSchemaName("").setTableName("").setEntityVersion(1)
                .setFkEntity(statementThreeId).setFkProject(projectId).setIsInProject(true).build();
        proInfoProjRelTopic.pipeInput(kR, vR);


        // add statement four
        kS = ts.information.statement.Key.newBuilder().setPkEntity(statementFourId).build();
        vS = StatementEnrichedValue.newBuilder().setSubjectId(entityName2).setPropertyId(propertyHasStringId)
                .setObjectLabel("Name 2")
                .setObject(NodeValue.newBuilder().setLabel("Name 2").setId("").setClassId(0).build()).build();
        statementWithLiteralTopic.pipeInput(kS, vS);

        // add relation between project and statement four
        kR = ts.projects.info_proj_rel.Key.newBuilder().setFkEntity(statementFourId).setFkProject(projectId).build();
        vR = ts.projects.info_proj_rel.Value.newBuilder().setSchemaName("").setTableName("").setEntityVersion(1)
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
        assertThat(outRecords).hasSize(4);

    }


}
