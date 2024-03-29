package org.geovistory.toolbox.streams.entity.label.processors.community;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label.*;
import org.geovistory.toolbox.streams.entity.label.processsors.community.CommunityToolboxStatementWithLiteral;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CommunityToolboxStatementWithLiteralTest {

    private static final String SCHEMA_REGISTRY_SCOPE = CommunityToolboxStatementWithLiteralTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectStatementKey, ProjectStatementValue> projectEntityTopic;
    private TestOutputTopic<CommunityStatementKey, CommunityStatementValue> outputTopic;

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
        var communityClassLabel = new CommunityToolboxStatementWithLiteral(avroSerdes, registerInputTopic, registerInnerTopic, outputTopicNames);
        communityClassLabel.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        projectEntityTopic = testDriver.createInputTopic(
                outputTopicNames.projectStatementWithLiteral(),
                avroSerdes.ProjectStatementKey().serializer(),
                avroSerdes.ProjectStatementValue().serializer());


        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.communityToolboxStatementWithLiteral(),
                avroSerdes.CommunityStatementKey().deserializer(),
                avroSerdes.CommunityStatementValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }


    @Test
    public void testAddStatementToTwoProjects() {

        var key1 = ProjectStatementKey.newBuilder()
                .setStatementId(1).setProjectId(1).build();
        var value1 = ProjectStatementValue.newBuilder()
                .setStatementId(1)
                .setProjectId(1)
                .setStatement(
                        StatementEnrichedValue.newBuilder().setSubjectId("i1").setPropertyId(2)

                                .setSubject(
                                        NodeValue.newBuilder().setLabel("Name 2").setId("i1").setClassId(0)
                                                .setEntity(
                                                        Entity.newBuilder()
                                                                .setFkClass(1)
                                                                .setCommunityVisibilityWebsite(false)
                                                                .setCommunityVisibilityDataApi(false)
                                                                .setCommunityVisibilityToolbox(true)
                                                                .build())
                                                .build()
                                )
                                .setObjectLabel("i2")
                                .setObject(NodeValue.newBuilder().setLabel("Name 2").setId("i2").setClassId(0)
                                        .setAppellation(
                                                Appellation.newBuilder()
                                                        .setFkClass(1)
                                                        .setString("asd")
                                                        .build())
                                        .build()).build()
                )
                .setOrdNumOfDomain(1)
                .setOrdNumOfRange(2)
                .setDeleted$1(false)
                .build();

        // add to project 1
        projectEntityTopic.pipeInput(key1, value1);

        // add to project 2
        value1.setProjectId(2);
        projectEntityTopic.pipeInput(key1, value1);

        // passing the same key-value pair again, should not change the count
        projectEntityTopic.pipeInput(key1, value1);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = CommunityStatementKey.newBuilder()
                .setStatementId(1)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getProjectCount()).isEqualTo(2);

    }


    @Test
    public void testRemoveEntityFromProject() {

        var key1 = ProjectStatementKey.newBuilder()
                .setStatementId(1).setProjectId(1).build();
        var value1 = ProjectStatementValue.newBuilder()
                .setStatementId(1)
                .setProjectId(1)
                .setStatement(
                        StatementEnrichedValue.newBuilder().setSubjectId("i1").setPropertyId(2)

                                .setSubject(
                                        NodeValue.newBuilder().setLabel("Name 2").setId("i1").setClassId(0)
                                                .setEntity(
                                                        Entity.newBuilder()
                                                                .setFkClass(1)
                                                                .setCommunityVisibilityWebsite(false)
                                                                .setCommunityVisibilityDataApi(false)
                                                                .setCommunityVisibilityToolbox(true)
                                                                .build())
                                                .build()
                                )
                                .setObjectLabel("i2")
                                .setObject(NodeValue.newBuilder().setLabel("Name 2").setId("i2").setClassId(0)
                                        .setAppellation(
                                                Appellation.newBuilder()
                                                        .setFkClass(1)
                                                        .setString("asd")
                                                        .build())
                                        .build()).build()
                )
                .setOrdNumOfDomain(1)
                .setOrdNumOfRange(2)
                .setDeleted$1(false)
                .build();


        // add to project 1
        projectEntityTopic.pipeInput(key1, value1);

        // add to project 2
        value1.setProjectId(2);
        projectEntityTopic.pipeInput(key1, value1);

        // remove from project 2
        value1.setDeleted$1(true);
        projectEntityTopic.pipeInput(key1, value1);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = CommunityStatementKey.newBuilder()
                .setStatementId(1)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getProjectCount()).isEqualTo(1);

    }


    @Test
    public void testEntityNotVisibleForCommunity() {

        var key1 = ProjectStatementKey.newBuilder()
                .setStatementId(1).setProjectId(1).build();
        var value1 = ProjectStatementValue.newBuilder()
                .setStatementId(1)
                .setProjectId(1)
                .setStatement(
                        StatementEnrichedValue.newBuilder().setSubjectId("i1").setPropertyId(2)

                                .setSubject(
                                        NodeValue.newBuilder().setLabel("Name 2").setId("i1").setClassId(0)
                                                .setEntity(
                                                        Entity.newBuilder()
                                                                .setFkClass(1)
                                                                .setCommunityVisibilityWebsite(false)
                                                                .setCommunityVisibilityDataApi(false)
                                                                .setCommunityVisibilityToolbox(false)
                                                                .build())
                                                .build()
                                )
                                .setObjectLabel("i2")
                                .setObject(NodeValue.newBuilder().setLabel("Name 2").setId("i2").setClassId(0)
                                        .setAppellation(
                                                Appellation.newBuilder()
                                                        .setFkClass(1)
                                                        .setString("asd")
                                                        .build())
                                        .build()).build()
                )
                .setOrdNumOfDomain(1)
                .setOrdNumOfRange(2)
                .setDeleted$1(false)
                .build();

        // add to project 1
        projectEntityTopic.pipeInput(key1, value1);

        // add to project 2
        value1.setProjectId(2);
        projectEntityTopic.pipeInput(key1, value1);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = CommunityStatementKey.newBuilder()
                .setStatementId(1)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getProjectCount()).isEqualTo(0);

    }


}
