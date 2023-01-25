package org.geovistory.toolbox.streams.topology;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.topologies.ProjectTopIncomingStatements;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectTopIncomingStatementsTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectTopIncomingStatementsTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectStatementKey, ProjectStatementValue> projectStatementTopic;
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

        projectStatementTopic = testDriver.createInputTopic(
                ProjectTopIncomingStatements.input.TOPICS.project_statement,
                avroSerdes.ProjectStatementKey().serializer(),
                avroSerdes.ProjectStatementValue().serializer());


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
                .setOrdNumForRange(3)
                .build();
        projectStatementTopic.pipeInput(k, v);

        v.setStatementId(1);
        v.setOrdNumForRange(1);

        projectStatementTopic.pipeInput(k, v);

        v.setStatementId(2);
        v.setOrdNumForRange(2);
        projectStatementTopic.pipeInput(k, v);

        v.setStatementId(0);
        v.setOrdNumForRange(0);
        projectStatementTopic.pipeInput(k, v);

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
        assertThat(record.getStatements().get(2).getOrdNumForRange()).isEqualTo(2);
    }


}
