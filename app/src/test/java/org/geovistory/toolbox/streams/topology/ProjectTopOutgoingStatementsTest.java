package org.geovistory.toolbox.streams.topology;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.topologies.ProjectTopOutgoingStatements;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectTopOutgoingStatementsTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectTopOutgoingStatementsTest.class.getName();
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

        Topology topology = ProjectTopOutgoingStatements.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        projectStatementTopic = testDriver.createInputTopic(
                ProjectTopOutgoingStatements.input.TOPICS.project_statement,
                avroSerdes.ProjectStatementKey().serializer(),
                avroSerdes.ProjectStatementValue().serializer());


        outputTopic = testDriver.createOutputTopic(
                ProjectTopOutgoingStatements.output.TOPICS.project_top_outgoing_statements,
                avroSerdes.ProjectTopStatementsKey().deserializer(),
                avroSerdes.ProjectTopStatementsValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }


    @Test
    void testValueAggregatorOrdering() {
        var s = StatementEnrichedValue.newBuilder()
                .setSubjectId("1")
                .setPropertyId(2)
                .setObjectId("3")
                .build();

        var b = ProjectStatementValue.newBuilder();
        var v0 = new ArrayList<ProjectStatementValue>();
        var v1 = ProjectTopOutgoingStatements.addStatement(v0,
                b.setProjectId(1).setStatementId(4).setStatement(s).setOrdNumForDomain(4).build()
        );
        var v2 = ProjectTopOutgoingStatements.addStatement(v1,
                b.setProjectId(1).setStatementId(3).setStatement(s).setOrdNumForDomain(3).build()
        );
        var v3 = ProjectTopOutgoingStatements.addStatement(v2,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumForDomain(2).build()
        );
        var v4 = ProjectTopOutgoingStatements.addStatement(v3,
                b.setProjectId(1).setStatementId(5).setStatement(s).setOrdNumForDomain(null).build()
        );
        var v5 = ProjectTopOutgoingStatements.addStatement(v4,
                b.setProjectId(1).setStatementId(0).setStatement(s).setOrdNumForDomain(0).build()
        );
        var v6 = ProjectTopOutgoingStatements.addStatement(v5,
                b.setProjectId(1).setStatementId(1).setStatement(s).setOrdNumForDomain(1).build()
        );

        assertThat(v6.size()).isEqualTo(5);
        assertThat(v6.get(0).getOrdNumForDomain()).isEqualTo(0);
        assertThat(v6.get(3).getOrdNumForDomain()).isEqualTo(3);
    }

    @Test
    void testValueAggregatorMoveStatementDown() {
        var s = StatementEnrichedValue.newBuilder()
                .setSubjectId("1")
                .setPropertyId(2)
                .setObjectId("3")
                .build();

        var b = ProjectStatementValue.newBuilder();
        var v0 = new ArrayList<ProjectStatementValue>();
        var v1 = ProjectTopOutgoingStatements.addStatement(v0,
                b.setProjectId(1).setStatementId(1).setStatement(s).setOrdNumForDomain(2).build()
        );
        var v2 = ProjectTopOutgoingStatements.addStatement(v1,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumForDomain(1).build()
        );

        assertThat(v2.size()).isEqualTo(2);
        assertThat(v2.get(0).getStatementId()).isEqualTo(2);
        assertThat(v2.get(1).getStatementId()).isEqualTo(1);

        var v3 = ProjectTopOutgoingStatements.addStatement(v2,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumForDomain(3).build()
        );

        assertThat(v3.size()).isEqualTo(2);
        assertThat(v3.get(0).getStatementId()).isEqualTo(1);
        assertThat(v3.get(1).getStatementId()).isEqualTo(2);

    }

    @Test
    void testValueAggregatorMoveStatementUp() {
        var s = StatementEnrichedValue.newBuilder()
                .setSubjectId("1")
                .setPropertyId(2)
                .setObjectId("3")
                .build();

        var b = ProjectStatementValue.newBuilder();
        var v0 = new ArrayList<ProjectStatementValue>();
        var v1 = ProjectTopOutgoingStatements.addStatement(v0,
                b.setProjectId(1).setStatementId(1).setStatement(s).setOrdNumForDomain(2).build()
        );
        var v2 = ProjectTopOutgoingStatements.addStatement(v1,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumForDomain(3).build()
        );

        assertThat(v2.size()).isEqualTo(2);
        assertThat(v2.get(0).getStatementId()).isEqualTo(1);
        assertThat(v2.get(1).getStatementId()).isEqualTo(2);

        var v3 = ProjectTopOutgoingStatements.addStatement(v2,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumForDomain(1).build()
        );

        assertThat(v3.size()).isEqualTo(2);
        assertThat(v3.get(0).getStatementId()).isEqualTo(2);
        assertThat(v3.get(1).getStatementId()).isEqualTo(1);

    }

    @Test
    void testValueAggregatorDeleteStatementUp() {
        var s = StatementEnrichedValue.newBuilder()
                .setSubjectId("1")
                .setPropertyId(2)
                .setObjectId("3")
                .build();

        var b = ProjectStatementValue.newBuilder();
        var v0 = new ArrayList<ProjectStatementValue>();
        var v1 = ProjectTopOutgoingStatements.addStatement(v0,
                b.setProjectId(1).setStatementId(1).setStatement(s).setOrdNumForDomain(1).build()
        );
        var v2 = ProjectTopOutgoingStatements.addStatement(v1,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumForDomain(2).build()
        );
        var v3 = ProjectTopOutgoingStatements.addStatement(v2,
                b.setProjectId(1).setStatementId(3).setStatement(s).setOrdNumForDomain(3).build()
        );
        assertThat(v3.get(0).getStatementId()).isEqualTo(1);

        var v4 = ProjectTopOutgoingStatements.addStatement(v3,
                b.setProjectId(1).setStatementId(1).setStatement(s).setOrdNumForDomain(3).setDeleted$1(true).build()
        );

        assertThat(v4.size()).isEqualTo(2);
        assertThat(v4.get(0).getStatementId()).isEqualTo(2);
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
                .setOrdNumForDomain(3)
                .build();
        projectStatementTopic.pipeInput(k, v);

        v.setStatementId(1);
        v.setOrdNumForDomain(1);

        projectStatementTopic.pipeInput(k, v);

        v.setStatementId(2);
        v.setOrdNumForDomain(2);
        projectStatementTopic.pipeInput(k, v);

        v.setStatementId(0);
        v.setOrdNumForDomain(0);
        projectStatementTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultKey = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId)
                .setEntityId(subjectId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .build();
        var record = outRecords.get(resultKey);
        assertThat(record.getStatements().size()).isEqualTo(4);
        assertThat(record.getStatements().get(2).getOrdNumForDomain()).isEqualTo(2);
    }


}
