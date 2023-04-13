package org.geovistory.toolbox.streams.entity.processors.project;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectEntityTypeTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectEntityTypeTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<HasTypePropertyKey, HasTypePropertyValue> hasTypePropertyTopic;
    private TestInputTopic<ProjectEntityKey, ProjectEntityValue> projectEntityTopic;
    private TestInputTopic<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopOutgoingStatements;
    private TestOutputTopic<ProjectEntityKey, ProjectEntityTypeValue> outputTopic;

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
        var communityClassLabel = new ProjectEntityType(avroSerdes, registerInputTopic, outputTopicNames);
        communityClassLabel.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        hasTypePropertyTopic = testDriver.createInputTopic(
                inputTopicNames.getHasTypeProperty(),
                avroSerdes.HasTypePropertyKey().serializer(),
                avroSerdes.HasTypePropertyValue().serializer());

        projectTopOutgoingStatements = testDriver.createInputTopic(
                inputTopicNames.getProjectTopOutgoingStatements(),
                avroSerdes.ProjectTopStatementsKey().serializer(),
                avroSerdes.ProjectTopStatementsValue().serializer());

        projectEntityTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectEntity(),
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectEntityType(),
                avroSerdes.ProjectEntityKey().deserializer(),
                avroSerdes.ProjectEntityTypeValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }


    @Test
    void testProjectEntityType() {

        var entityId = "i1";
        var projectId = 2;
        var classId = 3;


        // add an entity
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(projectId).build();
        var vE = ProjectEntityValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setClassId(3).build();
        projectEntityTopic.pipeInput(kE, vE);

        var kC = HasTypePropertyKey.newBuilder().setClassId(classId).build();
        var vC = HasTypePropertyValue.newBuilder()
                .setClassId(classId)
                .setPropertyId(987)
                .build();
        hasTypePropertyTopic.pipeInput(kC, vC);


        var kS = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId).setEntityId(entityId)
                .setPropertyId(987).setIsOutgoing(true).build();
        var vS = ProjectTopStatementsValue.newBuilder()
                .setProjectId(projectId).setEntityId(entityId)
                .setPropertyId(987).setIsOutgoing(true)
                .setStatements(List.of(
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(1)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setPropertyId(987)
                                        .setObjectId("i654")
                                        .setObjectLabel("Joy")
                                        .build()).build()
                )).build();
        projectTopOutgoingStatements.pipeInput(kS, vS);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectEntityKey.newBuilder()
                .setEntityId(entityId)
                .setProjectId(projectId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getTypeId()).isEqualTo("i654");
        assertThat(record.getTypeLabel()).isEqualTo("Joy");

    }

    @Test
    void testProjectEntityTypeDeleteHasTypeProp() {

        var entityId = "i1";
        var projectId = 2;
        var classId = 3;


        // add an entity
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(projectId).build();
        var vE = ProjectEntityValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setClassId(3).build();
        projectEntityTopic.pipeInput(kE, vE);

        var kC = HasTypePropertyKey.newBuilder().setClassId(classId).build();
        var vC = HasTypePropertyValue.newBuilder()
                .setClassId(classId)
                .setPropertyId(987)
                .build();
        hasTypePropertyTopic.pipeInput(kC, vC);


        var kS = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId).setEntityId(entityId)
                .setPropertyId(987).setIsOutgoing(true).build();
        var vS = ProjectTopStatementsValue.newBuilder()
                .setProjectId(projectId).setEntityId(entityId)
                .setPropertyId(987).setIsOutgoing(true)
                .setStatements(List.of(
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(1)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setPropertyId(987)
                                        .setObjectId("i654")
                                        .setObjectLabel("Joy")
                                        .build()).build()
                )).build();
        projectTopOutgoingStatements.pipeInput(kS, vS);

        vC.setDeleted$1(true);
        hasTypePropertyTopic.pipeInput(kC, vC);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectEntityKey.newBuilder()
                .setEntityId(entityId)
                .setProjectId(projectId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getDeleted$1()).isEqualTo(true);

    }

    @Test
    void testProjectEntityTypeDeleteStatement() {

        var entityId = "i1";
        var projectId = 2;
        var classId = 3;


        // add an entity
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(projectId).build();
        var vE = ProjectEntityValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setClassId(3).build();
        projectEntityTopic.pipeInput(kE, vE);

        var kC = HasTypePropertyKey.newBuilder().setClassId(classId).build();
        var vC = HasTypePropertyValue.newBuilder()
                .setClassId(classId)
                .setPropertyId(987)
                .build();
        hasTypePropertyTopic.pipeInput(kC, vC);


        var kS = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId).setEntityId(entityId)
                .setPropertyId(987).setIsOutgoing(true).build();
        var vS = ProjectTopStatementsValue.newBuilder()
                .setProjectId(projectId).setEntityId(entityId)
                .setPropertyId(987).setIsOutgoing(true)
                .setStatements(List.of(
                        ProjectStatementValue.newBuilder().setProjectId(projectId).setStatementId(1)
                                .setOrdNumOfDomain(1)
                                .setStatement(StatementEnrichedValue.newBuilder()
                                        .setSubjectId(entityId)
                                        .setPropertyId(987)
                                        .setObjectId("i654")
                                        .setObjectLabel("Joy")
                                        .build()).build()
                )).build();

        projectTopOutgoingStatements.pipeInput(kS, vS);

        vS.setStatements(List.of());
        projectTopOutgoingStatements.pipeInput(kS, vS);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectEntityKey.newBuilder()
                .setEntityId(entityId)
                .setProjectId(projectId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getDeleted$1()).isEqualTo(true);

    }

}
