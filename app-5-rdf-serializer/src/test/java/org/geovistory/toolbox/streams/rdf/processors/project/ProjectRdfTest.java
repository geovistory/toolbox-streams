package org.geovistory.toolbox.streams.rdf.processors.project;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.rdf.Env;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectRdfTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectRdfTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityTopic;
    private TestOutputTopic<ProjectRdfKey, ProjectRdfValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = ProjectStatementToUri.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();
        projectStatementWithEntityTopic = testDriver.createInputTopic(
                Env.INSTANCE.TOPIC_PROJECT_STATEMENT_WITH_ENTITY,
                avroSerdes.ProjectStatementKey().serializer(),
                avroSerdes.ProjectStatementValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                ProjectStatementToUri.output.TOPICS.project_rdf,
                avroSerdes.ProjectRdfKey().deserializer(),
                avroSerdes.ProjectRdfValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }


    @Test
    void testProcessor() {

        var projectId = 1;
        var statementId = 123212;
        var subjectId = "i1";
        var objectId = "i2";
        var propertyId = 9;
        var k = ProjectStatementKey.newBuilder().setProjectId(projectId).setStatementId(statementId).build();
        var v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(statementId)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setObjectId(objectId)
                                .setPropertyId(propertyId)
                                .build())
                .setDeleted$1(false)
                .build();
        projectStatementWithEntityTopic.pipeInput(k, v);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);

        var expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<s> <p> <o>")
                .build();

        var record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<o> <p i> <s>")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

    }

}
