package org.geovistory.toolbox.streams.entity.label.processors.project;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label.processsors.project.ProjectTopOutgoingStatements;
import org.geovistory.toolbox.streams.entity.label.processsors.project.TopStatementAdder;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectTopOutgoingStatementsTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectTopOutgoingStatementsTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectStatementKey, ProjectStatementValue> projectStatementTopic;
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

        Topology topology = ProjectTopOutgoingStatements.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        projectStatementTopic = testDriver.createInputTopic(
                ProjectTopOutgoingStatements.input.TOPICS.project_statement,
                avroSerdes.ProjectStatementKey().serializer(),
                avroSerdes.ProjectStatementValue().serializer());

        projectEntityLabelTopic = testDriver.createInputTopic(
                ProjectTopOutgoingStatements.input.TOPICS.project_entity_label,
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityLabelValue().serializer());

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
        var v1 = TopStatementAdder.addStatement(v0,
                b.setProjectId(1).setStatementId(4).setStatement(s).setOrdNumOfRange(4).build(), true
        );
        var v2 = TopStatementAdder.addStatement(v1,
                b.setProjectId(1).setStatementId(3).setStatement(s).setOrdNumOfRange(3).build(), true
        );
        var v3 = TopStatementAdder.addStatement(v2,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumOfRange(2).build(), true
        );
        var v4 = TopStatementAdder.addStatement(v3,
                b.setProjectId(1).setStatementId(5).setStatement(s).setOrdNumOfRange(null).build(), true
        );
        var v5 = TopStatementAdder.addStatement(v4,
                b.setProjectId(1).setStatementId(0).setStatement(s).setOrdNumOfRange(0).build(), true
        );
        var v6 = TopStatementAdder.addStatement(v5,
                b.setProjectId(1).setStatementId(1).setStatement(s).setOrdNumOfRange(1).build(), true
        );

        assertThat(v6.size()).isEqualTo(5);
        assertThat(v6.get(0).getOrdNumOfRange()).isEqualTo(0);
        assertThat(v6.get(3).getOrdNumOfRange()).isEqualTo(3);
    }

    @Test
    void testValueAggregatorOrderingByModificationDate() {
        var s = StatementEnrichedValue.newBuilder()
                .setSubjectId("1")
                .setPropertyId(2)
                .setObjectId("3")
                .build();

        var b = ProjectStatementValue.newBuilder();
        var v0 = new ArrayList<ProjectStatementValue>();
        var v1 = TopStatementAdder.addStatement(v0,
                b.setProjectId(1).setStatementId(3).setStatement(s).setModifiedAt("2020-03-03T09:25:57.698128Z").build(), true
        );
        var v2 = TopStatementAdder.addStatement(v1,
                b.setProjectId(1).setStatementId(4).setStatement(s).setModifiedAt("2020-02-03T09:25:57.698128Z").build(), true
        );
        var v3 = TopStatementAdder.addStatement(v2,
                b.setProjectId(1).setStatementId(0).setStatement(s).setModifiedAt("2020-12-03T09:25:57.698128Z").build(), true
        );
        var v4 = TopStatementAdder.addStatement(v3,
                b.setProjectId(1).setStatementId(1).setStatement(s).setModifiedAt("2020-11-03T09:25:57.698128Z").build(), true
        );
        var v5 = TopStatementAdder.addStatement(v4,
                b.setProjectId(1).setStatementId(2).setStatement(s).setModifiedAt("2020-04-03T09:25:57.698128Z").build(), true
        );
        var v6 = TopStatementAdder.addStatement(v5,
                b.setProjectId(1).setStatementId(5).setStatement(s).setModifiedAt("2020-01-03T09:25:57.698128Z").build(), true
        );

        assertThat(v6.size()).isEqualTo(5);
        assertThat(v6.get(0).getStatementId()).isEqualTo(0);
        assertThat(v6.get(1).getStatementId()).isEqualTo(1);
        assertThat(v6.get(2).getStatementId()).isEqualTo(2);
        assertThat(v6.get(3).getStatementId()).isEqualTo(3);
        assertThat(v6.get(4).getStatementId()).isEqualTo(4);
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
        var v1 = TopStatementAdder.addStatement(v0,
                b.setProjectId(1).setStatementId(1).setStatement(s).setOrdNumOfRange(2).build(), true
        );
        var v2 = TopStatementAdder.addStatement(v1,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumOfRange(1).build(), true
        );

        assertThat(v2.size()).isEqualTo(2);
        assertThat(v2.get(0).getStatementId()).isEqualTo(2);
        assertThat(v2.get(1).getStatementId()).isEqualTo(1);

        var v3 = TopStatementAdder.addStatement(v2,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumOfRange(3).build(), true
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
        var v1 = TopStatementAdder.addStatement(v0,
                b.setProjectId(1).setStatementId(1).setStatement(s).setOrdNumOfRange(2).build(), true
        );
        var v2 = TopStatementAdder.addStatement(v1,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumOfRange(3).build(), true
        );

        assertThat(v2.size()).isEqualTo(2);
        assertThat(v2.get(0).getStatementId()).isEqualTo(1);
        assertThat(v2.get(1).getStatementId()).isEqualTo(2);

        var v3 = TopStatementAdder.addStatement(v2,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumOfRange(1).build(), true
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
        var v1 = TopStatementAdder.addStatement(v0,
                b.setProjectId(1).setStatementId(1).setStatement(s).setOrdNumOfRange(1).build(), true
        );
        var v2 = TopStatementAdder.addStatement(v1,
                b.setProjectId(1).setStatementId(2).setStatement(s).setOrdNumOfRange(2).build(), true
        );
        var v3 = TopStatementAdder.addStatement(v2,
                b.setProjectId(1).setStatementId(3).setStatement(s).setOrdNumOfRange(3).build(), true
        );
        assertThat(v3.get(0).getStatementId()).isEqualTo(1);

        var v4 = TopStatementAdder.addStatement(v3,
                b.setProjectId(1).setStatementId(1).setStatement(s).setOrdNumOfRange(3).setDeleted$1(true).build(), true
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
                                .setSubjectClassId(7)
                                .setPropertyId(propertyId)
                                .setObjectId(objectId)
                                .setObjectClassId(8)
                                .build()
                )
                .setOrdNumOfRange(3)
                .build();
        projectStatementTopic.pipeInput(k, v);

        v.setStatementId(1);
        v.setOrdNumOfRange(1);

        projectStatementTopic.pipeInput(k, v);

        v.setStatementId(2);
        v.setOrdNumOfRange(2);
        projectStatementTopic.pipeInput(k, v);

        v.setStatementId(0);
        v.setOrdNumOfRange(0);
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
        assertThat(record.getStatements().get(2).getOrdNumOfRange()).isEqualTo(2);
        assertThat(record.getClassId()).isEqualTo(7);
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
        projectStatementTopic.pipeInput(k, v);

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
                .setIsOutgoing(true)
                .setEntityId(subjectId)
                .setPropertyId(propertyId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getStatements().get(0).getStatement().getSubjectLabel()).isEqualTo(null);
        assertThat(record.getStatements().get(0).getStatement().getObjectLabel()).isEqualTo("Maria");
    }
}
