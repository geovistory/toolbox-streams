package org.geovistory.toolbox.streams.entity.label.processors.project;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label.AvroSerdes;
import org.geovistory.toolbox.streams.entity.label.InputTopicNames;
import org.geovistory.toolbox.streams.entity.label.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.label.processsors.project.ProjectTopStatementsInAndOut;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectTopStatementsInAndOutTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectTopStatementsInAndOutTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityTopic;

    private TestInputTopic<ProjectStatementKey, ProjectStatementValue> projectStatementWithLiteralTopic;
    private TestInputTopic<ProjectEntityKey, ProjectEntityLabelValue> projectEntityLabelTopic;
    private TestInputTopic<CommunityEntityKey, CommunityEntityLabelValue> communityEntityLabelTopic;
    private TestOutputTopic<ProjectTopStatementsKey, ProjectTopStatementsValue> topIncomingTopic;
    private TestOutputTopic<ProjectTopStatementsKey, ProjectTopStatementsValue> topOutgoingTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");

        var avroSerdes = new AvroSerdes();
        avroSerdes.QUARKUS_KAFKA_STREAMS_SCHEMA_REGISTRY_URL = MOCK_SCHEMA_REGISTRY_URL;
        var inputTopicNames = new InputTopicNames();
        var outputTopicNames = new OutputTopicNames();
        var projectTopIncomingStatements = new ProjectTopStatementsInAndOut(avroSerdes, inputTopicNames, outputTopicNames);
        var topology = new Topology();
        projectTopIncomingStatements.standalone(topology);
        testDriver = new TopologyTestDriver(topology, props);

        projectStatementWithEntityTopic = testDriver.createInputTopic(
                outputTopicNames.projectStatementWithEntity(),
                avroSerdes.ProjectStatementKey().serializer(),
                avroSerdes.ProjectStatementValue().serializer());

        projectStatementWithLiteralTopic = testDriver.createInputTopic(
                outputTopicNames.projectStatementWithLiteral(),
                avroSerdes.ProjectStatementKey().serializer(),
                avroSerdes.ProjectStatementValue().serializer());

        projectEntityLabelTopic = testDriver.createInputTopic(
                outputTopicNames.projectEntityLabel(),
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityLabelValue().serializer());

        communityEntityLabelTopic = testDriver.createInputTopic(
                outputTopicNames.communityToolboxEntityLabel(),
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.CommunityEntityLabelValue().serializer());

        topIncomingTopic = testDriver.createOutputTopic(
                outputTopicNames.projectTopIncomingStatements(),
                avroSerdes.ProjectTopStatementsKey().deserializer(),
                avroSerdes.ProjectTopStatementsValue().deserializer());


        topOutgoingTopic = testDriver.createOutputTopic(
                outputTopicNames.projectTopOutgoingStatements(),
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
                                .setSubjectClassId(7)
                                .setPropertyId(propertyId)
                                .setObjectId(objectId)
                                .setObjectClassId(8)
                                .setSubjectId(subjectId)
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

        assertThat(topIncomingTopic.isEmpty()).isFalse();
        var outRecords = topIncomingTopic.readKeyValuesToMap();
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
        assertThat(record.getClassId()).isEqualTo(8);
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

        assertThat(topIncomingTopic.isEmpty()).isFalse();
        var outRecords = topIncomingTopic.readKeyValuesToMap();
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


    @Test
    void testUpdateEntityLabels() {
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

        vSE.setLabel("Jack2");
        projectEntityLabelTopic.pipeInput(kSE, vSE);

        assertThat(topIncomingTopic.isEmpty()).isFalse();
        var outRecords = topIncomingTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId)
                .setIsOutgoing(false)
                .setEntityId(objectId)
                .setPropertyId(propertyId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getStatements().get(0).getStatement().getSubjectLabel()).isEqualTo("Jack2");
        assertThat(record.getStatements().get(0).getStatement().getObjectLabel()).isEqualTo(null);
    }


    @Test
    void testJoinEntityLabelsDeleteAndAdd() {
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


        // add subject community entity label
        var kCE = CommunityEntityKey.newBuilder().setEntityId(subjectId).build();
        var vCE = CommunityEntityLabelValue.newBuilder().setEntityId(subjectId)
                .setLabelSlots(List.of("")).setLabel("Jack Community").build();
        communityEntityLabelTopic.pipeInput(kCE, vCE);

        // add subject project entity label
        var kSE = ProjectEntityKey.newBuilder().setEntityId(subjectId).setProjectId(projectId).build();
        var vSE = ProjectEntityLabelValue.newBuilder().setEntityId(subjectId).setProjectId(projectId)
                .setLabelSlots(List.of("")).setLabel("Jack").build();
        projectEntityLabelTopic.pipeInput(kSE, vSE);

        // delete subject project entity label
        vSE.setDeleted$1(true);
        projectEntityLabelTopic.pipeInput(kSE, vSE);

        // delete subject communty entity label
        vCE.setDeleted$1(true);
        communityEntityLabelTopic.pipeInput(kCE, vCE);

        // add object entity label
        var kOE = ProjectEntityKey.newBuilder().setEntityId(objectId).setProjectId(projectId).build();
        var vOE = ProjectEntityLabelValue.newBuilder().setEntityId(objectId).setProjectId(projectId)
                .setLabelSlots(List.of("")).setLabel("Maria").build();
        projectEntityLabelTopic.pipeInput(kOE, vOE);

        assertThat(topIncomingTopic.isEmpty()).isFalse();
        var incomingRecords = topIncomingTopic.readRecordsToList();
        assertThat(incomingRecords).hasSize(5);

        assertThat(incomingRecords.get(0).getValue().getStatements().get(0).getStatement().getSubjectLabel()).isEqualTo(null);
        assertThat(incomingRecords.get(1).getValue().getStatements().get(0).getStatement().getSubjectLabel()).isEqualTo("Jack Community");
        assertThat(incomingRecords.get(2).getValue().getStatements().get(0).getStatement().getSubjectLabel()).isEqualTo("Jack");
        assertThat(incomingRecords.get(3).getValue().getStatements().get(0).getStatement().getSubjectLabel()).isEqualTo("Jack Community");
        assertThat(incomingRecords.get(4).getValue().getStatements().get(0).getStatement().getSubjectLabel()).isEqualTo(null);

        var outgoingRecords = topOutgoingTopic.readRecordsToList();
        assertThat(outgoingRecords).hasSize(2);

        assertThat(outgoingRecords.get(0).getValue().getStatements().get(0).getStatement().getObjectLabel()).isEqualTo(null);
        assertThat(outgoingRecords.get(1).getValue().getStatements().get(0).getStatement().getObjectLabel()).isEqualTo("Maria");

    }

    @Test
    void testJoinCommunityEntityLabels() {
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
        var kSE = CommunityEntityKey.newBuilder().setEntityId(subjectId).build();
        var vSE = CommunityEntityLabelValue.newBuilder().setEntityId(subjectId)
                .setLabelSlots(List.of("")).setLabel("Jack").build();
        communityEntityLabelTopic.pipeInput(kSE, vSE);

        // add object entity label
        var kOE = ProjectEntityKey.newBuilder().setEntityId(objectId).setProjectId(projectId).build();
        var vOE = ProjectEntityLabelValue.newBuilder().setEntityId(objectId).setProjectId(projectId)
                .setLabelSlots(List.of("")).setLabel("Maria").build();
        projectEntityLabelTopic.pipeInput(kOE, vOE);

        assertThat(topIncomingTopic.isEmpty()).isFalse();
        var outRecords = topIncomingTopic.readKeyValuesToMap();
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


    @Test
    void testDeleteCommunityEntityLabels() {
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
        var kSE = CommunityEntityKey.newBuilder().setEntityId(subjectId).build();
        var vSE = CommunityEntityLabelValue.newBuilder().setEntityId(subjectId)
                .setLabelSlots(List.of("")).setLabel("Jack").build();
        communityEntityLabelTopic.pipeInput(kSE, vSE);

        // add object entity label
        var kOE = ProjectEntityKey.newBuilder().setEntityId(objectId).setProjectId(projectId).build();
        var vOE = ProjectEntityLabelValue.newBuilder().setEntityId(objectId).setProjectId(projectId)
                .setLabelSlots(List.of("")).setLabel("Maria").build();
        projectEntityLabelTopic.pipeInput(kOE, vOE);

        // mark subject entity label as deleted
        vSE.setDeleted$1(true);
        communityEntityLabelTopic.pipeInput(kSE, vSE);

        // send tombstone subject entity label
        communityEntityLabelTopic.pipeInput(kSE, null);

        assertThat(topIncomingTopic.isEmpty()).isFalse();
        var outRecords = topIncomingTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId)
                .setIsOutgoing(false)
                .setEntityId(objectId)
                .setPropertyId(propertyId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getStatements().get(0).getStatement().getSubjectLabel()).isEqualTo(null);
        assertThat(record.getStatements().get(0).getStatement().getObjectLabel()).isEqualTo(null);
    }

    @Test
    void testDeleteEntityLabels() {
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

        // mark subject entity label as deleted
        vSE.setDeleted$1(true);
        projectEntityLabelTopic.pipeInput(kSE, vSE);

        // send entity label as tombstone
        projectEntityLabelTopic.pipeInput(kSE, null);

        assertThat(topIncomingTopic.isEmpty()).isFalse();
        var outRecords = topIncomingTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId)
                .setIsOutgoing(false)
                .setEntityId(objectId)
                .setPropertyId(propertyId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getStatements().get(0).getStatement().getSubjectLabel()).isEqualTo(null);
        assertThat(record.getStatements().get(0).getStatement().getObjectLabel()).isEqualTo(null);
    }


    @Test
    void testDeleteStmt() {
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

        // mark stmt as deleted
        v.setDeleted$1(true);
        projectStatementWithEntityTopic.pipeInput(k, v);

        // send stmt as tombstone
        projectStatementWithEntityTopic.pipeInput(k, null);


        assertThat(topIncomingTopic.isEmpty()).isFalse();
        var outRecords = topIncomingTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectTopStatementsKey.newBuilder()
                .setProjectId(projectId)
                .setIsOutgoing(false)
                .setEntityId(objectId)
                .setPropertyId(propertyId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getStatements().size()).isEqualTo(0);
    }


    @Test
    void testJoinObjectEntityLabels() {
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

        assertThat(topOutgoingTopic.isEmpty()).isFalse();
        var outRecords = topOutgoingTopic.readKeyValuesToMap();
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


    @Test
    void testAggregateLiteralsAndEntityLabels() {
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


        // add statement wi
        k = ProjectStatementKey.newBuilder()
                .setProjectId(projectId)
                .setStatementId(2)
                .build();
        v = ProjectStatementValue.newBuilder()
                .setProjectId(projectId)
                .setStatementId(2)
                .setStatement(
                        StatementEnrichedValue.newBuilder()
                                .setSubjectId(subjectId)
                                .setPropertyId(propertyId)
                                .setObjectId(objectId)
                                .setObjectLabel("LITERAL")
                                .build()
                )
                .setOrdNumOfRange(4)
                .build();
        projectStatementWithLiteralTopic.pipeInput(k, v);


        assertThat(topOutgoingTopic.isEmpty()).isFalse();
        var outRecords = topOutgoingTopic.readKeyValuesToMap();
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
        assertThat(record.getStatements().get(1).getStatement().getObjectLabel()).isEqualTo("LITERAL");
    }
}
