package org.geovistory.toolbox.streams.project.entity.processors;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.project.entity.I;
import org.geovistory.toolbox.streams.project.entity.topologies.ProjectEntityTopStatements;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectEntityTopStatementsTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectEntityTopStatementsTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectEntityKey, ProjectEntityValue> projectEntityTopic;
    private TestInputTopic<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopStatementsTopic;
    private TestInputTopic<ProjectFieldLabelKey, ProjectFieldLabelValue> projectPropertyLabelTopic;
    private TestOutputTopic<ProjectEntityKey, ProjectEntityTopStatementsValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = ProjectEntityTopStatements.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        projectEntityTopic = testDriver.createInputTopic(
                ProjectEntityTopStatements.input.TOPICS.project_entity,
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityValue().serializer());

        projectPropertyLabelTopic = testDriver.createInputTopic(
                ProjectEntityTopStatements.input.TOPICS.project_property_label,
                avroSerdes.ProjectPropertyLabelKey().serializer(),
                avroSerdes.ProjectPropertyLabelValue().serializer());

        projectTopStatementsTopic = testDriver.createInputTopic(
                ProjectEntityTopStatements.input.TOPICS.project_top_statements,
                avroSerdes.ProjectTopStatementsKey().serializer(),
                avroSerdes.ProjectTopStatementsValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                ProjectEntityTopStatements.output.TOPICS.project_entity_top_statements,
                avroSerdes.ProjectEntityKey().deserializer(),
                avroSerdes.ProjectEntityTopStatementsValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testAddingProperties() {

        var projectId = 1;
        var entityId = "i1";
        var classId = 2;

        // add project entity
        var kE = ProjectEntityKey.newBuilder().setProjectId(projectId).setEntityId(entityId).build();
        var vE = ProjectEntityValue.newBuilder().setProjectId(projectId).setEntityId(entityId).setClassId(classId).build();
        projectEntityTopic.pipeInput(kE, vE);

        // add top statements
        var kT = ProjectTopStatementsKey.newBuilder().setProjectId(projectId).setEntityId(entityId).setPropertyId(1)
                .setIsOutgoing(true).build();
        var vT = ProjectTopStatementsValue.newBuilder().setProjectId(projectId).setEntityId(entityId).setPropertyId(1)
                .setIsOutgoing(true).setStatements(
                        List.of(
                                ProjectStatementValue.newBuilder().setProjectId(1).setStatementId(3).setStatement(
                                        StatementEnrichedValue.newBuilder()
                                                .setSubjectId("foo").setPropertyId(1).setObjectId("bar").build()
                                ).build()
                        )
                ).build();
        projectTopStatementsTopic.pipeInput(kT, vT);

        // add project property label
        var kP = ProjectFieldLabelKey.newBuilder().setProjectId(projectId).setPropertyId(1).setIsOutgoing(true)
                .setClassId(classId).build();
        var vP = ProjectFieldLabelValue.newBuilder().setProjectId(projectId).setPropertyId(1).setIsOutgoing(true)
                .setClassId(classId).setLabel("has").setLanguageId(I.EN.get()).build();
        projectPropertyLabelTopic.pipeInput(kP, vP);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultKey = ProjectEntityKey.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .build();
        var record = outRecords.get(resultKey);
        assertThat(record.getMap().size()).isEqualTo(1);
        assertThat(record.getMap().get("1_out").getStatements().size()).isEqualTo(1);
        assertThat(record.getMap().get("1_out").getPropertyLabel()).isEqualTo("has");
    }


}
