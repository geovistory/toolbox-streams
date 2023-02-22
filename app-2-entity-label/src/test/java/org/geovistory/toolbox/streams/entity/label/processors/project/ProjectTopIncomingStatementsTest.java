package org.geovistory.toolbox.streams.entity.label.processors.project;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label.processsors.project.ProjectTopIncomingStatements;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectTopIncomingStatementsTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectTopIncomingStatementsTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityTopic;

    private TestInputTopic<ProjectEntityKey, ProjectEntityLabelValue> projectEntityLabelTopic;
    private TestOutputTopic<ProjectTopStatementsKey, ProjectTopStatementsValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = ProjectTopIncomingStatements.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        projectStatementWithEntityTopic = testDriver.createInputTopic(
                ProjectTopIncomingStatements.input.TOPICS.project_statement_with_entity,
                avroSerdes.ProjectStatementKey().serializer(),
                avroSerdes.ProjectStatementValue().serializer());

        projectEntityLabelTopic = testDriver.createInputTopic(
                ProjectTopIncomingStatements.input.TOPICS.project_entity_label,
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityLabelValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                ProjectTopIncomingStatements.output.TOPICS.project_top_incoming_statements,
                avroSerdes.ProjectTopStatementsKey().deserializer(),
                avroSerdes.ProjectTopStatementsValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testFourStatementsOfSameSubjectAndProperty() {
        int projectId = 1;
        String subjectId = "i10";
        int propertyId = 20;
        String objectId = "i30";

        // add subject entity label
        var kOE = ProjectEntityKey.newBuilder().setEntityId(subjectId).setProjectId(projectId).build();
        var vOE = ProjectEntityLabelValue.newBuilder().setEntityId(subjectId).setProjectId(projectId)
                .setLabelSlots(List.of("")).setLabel("Maria").build();
        projectEntityLabelTopic.pipeInput(kOE, vOE);

        // add statement
        var k = ProjectStatementKey.newBuilder()
                .setProjectId(projectId)
                .setStatementId(1)
                .build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(1)
                .setStatementId(3)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setPropertyId(propertyId)
                                .setObjectId(objectId)
                                .build()
                )
                .setOrdNumOfDomain(3)
                .build();
        projectStatementWithEntityTopic.pipeInput(k, v);

        v.setStatementId(1);
        v.setOrdNumOfDomain(1);

        projectStatementWithEntityTopic.pipeInput(k, v);

        v.setStatementId(2);
        v.setOrdNumOfDomain(2);
        projectStatementWithEntityTopic.pipeInput(k, v);

        v.setStatementId(0);
        v.setOrdNumOfDomain(0);
        projectStatementWithEntityTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultKey = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId)
                .setEntityId(objectId)
                .setPropertyId(propertyId)
                .setIsOutgoing(false)
                .build();
        var record = outRecords.get(resultKey);
        assertThat(record.getStatements().size()).isEqualTo(4);
        assertThat(record.getStatements().get(2).getOrdNumOfDomain()).isEqualTo(2);
    }


    @Test
    void testJoinEntityLabels() {
        var projectId = 10;
        var propertyId = 30;
        var subjectId = "i1";
        var objectId = "i2";

        // add statement
        var k = ProjectStatementKey.newBuilder()
                .setProjectId(projectId)
                .setStatementId(1)
                .build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(3)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setPropertyId(propertyId)
                                .setObjectId(objectId)
                                .build()
                )
                .setOrdNumOfRange(3)
                .build();
        projectStatementWithEntityTopic.pipeInput(k, v);

        // add subject entity label
        var kSE = ProjectEntityKey.newBuilder().setEntityId(subjectId).setProjectId(projectId).build();
        var vSE = ProjectEntityLabelValue.newBuilder().setEntityId(subjectId).setProjectId(projectId)
                .setLabelSlots(List.of("")).setLabel("Jack").build();
        projectEntityLabelTopic.pipeInput(kSE, vSE);

        // add object entity label
        var kOE = ProjectEntityKey.newBuilder().setEntityId(objectId).setProjectId(projectId).build();
        var vOE = ProjectEntityLabelValue.newBuilder().setEntityId(objectId).setProjectId(projectId)
                .setLabelSlots(List.of("")).setLabel("Maria").build();
        projectEntityLabelTopic.pipeInput(kOE, vOE);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId)
                .setIsOutgoing(false)
                .setEntityId(objectId)
                .setPropertyId(propertyId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getStatements().get(0).getStatement().getSubjectLabel()).isEqualTo("Jack");
        assertThat(record.getStatements().get(0).getStatement().getObjectLabel()).isEqualTo(null);
    }
}
