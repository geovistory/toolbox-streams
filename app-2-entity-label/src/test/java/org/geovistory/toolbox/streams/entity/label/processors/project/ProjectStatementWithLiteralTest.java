package org.geovistory.toolbox.streams.entity.label.processors.project;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementValue;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.entity.label.*;
import org.geovistory.toolbox.streams.entity.label.processsors.project.ProjectStatementWithLiteral;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectStatementWithLiteralTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectStatementWithLiteralTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ts.information.statement.Key, StatementEnrichedValue> infStatementTopic;
    private TestInputTopic<ts.projects.info_proj_rel.Key, ts.projects.info_proj_rel.Value> proInfoProjRelTopic;
    private TestOutputTopic<ProjectStatementKey, ProjectStatementValue> outputTopic;

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
        var projectStatementWithLiteral = new ProjectStatementWithLiteral(avroSerdes, registerInputTopic, registerInnerTopic, outputTopicNames);
        projectStatementWithLiteral.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);


        infStatementTopic = testDriver.createInputTopic(
                inputTopicNames.getStatementWithLiteral(),
                avroSerdes.InfStatementKey().serializer(),
                avroSerdes.StatementEnrichedValue().serializer());

        proInfoProjRelTopic = testDriver.createInputTopic(
                inputTopicNames.proInfoProjRel(),
                avroSerdes.ProInfoProjRelKey().serializer(),
                avroSerdes.ProInfoProjRelValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectStatementWithLiteral(),
                avroSerdes.ProjectStatementKey().deserializer(),
                avroSerdes.ProjectStatementValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testOneStatementAndOneProjectRel() {
        var projectId = 10;
        var statementId = 20;
        var propertyId = 30;
        // add relation between project and statement
        var kR = ts.projects.info_proj_rel.Key.newBuilder()
                .setFkEntity(statementId)
                .setFkProject(projectId)
                .build();
        var vR = ts.projects.info_proj_rel.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(1)
                .setFkEntity(statementId)
                .setFkProject(projectId)
                .setIsInProject(true)
                .build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add statement
        var kE = ts.information.statement.Key.newBuilder().setPkEntity(statementId).build();
        var vE = StatementEnrichedValue.newBuilder()
                .setSubjectId("i" + 1)
                .setPropertyId(propertyId)
                .setObjectId("i" + 1)
                .build();
        infStatementTopic.pipeInput(kE, vE);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectStatementKey.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getDeleted$1()).isEqualTo(false);
        assertThat(record.getStatement().getPropertyId()).isEqualTo(propertyId);
    }

    @Test
    void testDeleteProjectRel() {
        var projectId = 10;
        var statementId = 20;
        var propertyId = 30;
        // add relation between project and statement
        var kR = ts.projects.info_proj_rel.Key.newBuilder()
                .setFkEntity(statementId)
                .setFkProject(projectId)
                .build();
        var vR = ts.projects.info_proj_rel.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(1)
                .setFkEntity(statementId)
                .setFkProject(projectId)
                .setIsInProject(true)
                .build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add statement
        var kE = ts.information.statement.Key.newBuilder().setPkEntity(statementId).build();
        var vE = StatementEnrichedValue.newBuilder()
                .setSubjectId("i" + 1)
                .setPropertyId(propertyId)
                .setObjectId("i" + 1)
                .build();
        infStatementTopic.pipeInput(kE, vE);

        vR.setDeleted$1("true");
        proInfoProjRelTopic.pipeInput(kR, vR);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectStatementKey.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .build();
        assertThat(outRecords.containsKey(resultingKey)).isTrue();
        var record = outRecords.get(resultingKey);
        assertThat(record.getDeleted$1()).isEqualTo(true);
    }

    @Test
    void testDeleteStatement() {
        var projectId = 10;
        var statementId = 20;
        var propertyId = 30;
        // add relation between project and statement
        var kR = ts.projects.info_proj_rel.Key.newBuilder()
                .setFkEntity(statementId)
                .setFkProject(projectId)
                .build();
        var vR = ts.projects.info_proj_rel.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(1)
                .setFkEntity(statementId)
                .setFkProject(projectId)
                .setIsInProject(true)
                .build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add statement
        var kE = ts.information.statement.Key.newBuilder().setPkEntity(statementId).build();
        var vE = StatementEnrichedValue.newBuilder()
                .setSubjectId("i" + 1)
                .setPropertyId(propertyId)
                .setObjectId("i" + 1)
                .build();
        infStatementTopic.pipeInput(kE, vE);

        vE.setDeleted$1(true);
        infStatementTopic.pipeInput(kE, vE);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectStatementKey.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .build();
        assertThat(outRecords.containsKey(resultingKey)).isTrue();
        var record = outRecords.get(resultingKey);
        assertThat(record.getDeleted$1()).isEqualTo(true);
    }

    @Test
    void testRemoveStatementFromProject() {
        var projectId = 10;
        var statementId = 20;
        var propertyId = 30;
        // add relation between project and statement
        var kR = ts.projects.info_proj_rel.Key.newBuilder()
                .setFkEntity(statementId)
                .setFkProject(projectId)
                .build();
        var vR = ts.projects.info_proj_rel.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(1)
                .setFkEntity(statementId)
                .setFkProject(projectId)
                .setIsInProject(true)
                .build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add statement
        var kE = ts.information.statement.Key.newBuilder().setPkEntity(statementId).build();
        var vE = StatementEnrichedValue.newBuilder()
                .setSubjectId("i" + 1)
                .setPropertyId(propertyId)
                .setObjectId("i" + 1)
                .build();
        infStatementTopic.pipeInput(kE, vE);

        vR.setIsInProject(false);
        proInfoProjRelTopic.pipeInput(kR, vR);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectStatementKey.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .build();
        assertThat(outRecords.containsKey(resultingKey)).isTrue();
        var record = outRecords.get(resultingKey);
        assertThat(record.getDeleted$1()).isEqualTo(true);
    }

    @Test
    void testTwoProjectsOneStatement() {
        var projectOneId = 10;
        var projectTwoId = 11;
        var statementId = 20;
        var propertyId = 30;

        // add relation between project one and statement
        var kR = ts.projects.info_proj_rel.Key.newBuilder()
                .setFkEntity(statementId)
                .setFkProject(projectOneId)
                .build();
        var vR = ts.projects.info_proj_rel.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(1)
                .setFkEntity(statementId)
                .setFkProject(projectOneId)
                .setIsInProject(true)
                .build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add relation between project two and statement
        kR = ts.projects.info_proj_rel.Key.newBuilder()
                .setFkEntity(statementId)
                .setFkProject(projectTwoId)
                .build();
        vR = ts.projects.info_proj_rel.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(1)
                .setFkEntity(statementId)
                .setFkProject(projectTwoId)
                .setIsInProject(true)
                .build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add statement
        var kE = ts.information.statement.Key.newBuilder().setPkEntity(statementId).build();
        var vE = StatementEnrichedValue.newBuilder()
                .setSubjectId("i" + 1)
                .setPropertyId(propertyId)
                .setObjectId("i" + 1)
                .build();
        infStatementTopic.pipeInput(kE, vE);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);
        var resultingKeyOne = ProjectStatementKey.newBuilder()
                .setProjectId(projectOneId)
                .setStatementId(statementId)
                .build();
        assertThat(outRecords.containsKey(resultingKeyOne)).isTrue();
        var resultingKeyTwo = ProjectStatementKey.newBuilder()
                .setProjectId(projectTwoId)
                .setStatementId(statementId)
                .build();
        assertThat(outRecords.containsKey(resultingKeyTwo)).isTrue();
    }


}
