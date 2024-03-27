package org.geovistory.toolbox.streams.project.items;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.project.items.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.project.items.names.OutputTopicNames;
import org.geovistory.toolbox.streams.testlib.FileRemover;
import org.geovistory.toolbox.streams.testlib.TopologyTestDriverProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
public class TestJoinCompleteStatement {
    @Inject
    Topology topology;
    @Inject
    ConfiguredAvroSerde as;
    @Inject
    OutputTopicNames outputTopicNames;
    @ConfigProperty(name = "kafka-streams.state.dir")
    public String stateDir;
    TopologyTestDriver testDriver;
    TestInputTopic<ProjectStatementKey, StatementWithSubValue> psWithSubInputTopic;
    TestInputTopic<ProjectStatementKey, StatementWithObValue> psWithObInputTopic;
    TestOutputTopic<String, EdgeValue> edgesOutputTopic;


    @BeforeEach
    public void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testApplicationId");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        as.schemaRegistryUrl = "mock://" + TestJoinCompleteStatement.class;

        testDriver = new TopologyTestDriver(topology, config);
        psWithSubInputTopic = testDriver.createInputTopic(
                outputTopicNames.projectStatementWithSubByPk(),
                as.<ProjectStatementKey>key().serializer(), as.<StatementWithSubValue>value().serializer()
        );
        psWithObInputTopic = testDriver.createInputTopic(
                outputTopicNames.projectStatementWithObByPk(),
                as.<ProjectStatementKey>key().serializer(), as.<StatementWithObValue>value().serializer()
        );
        edgesOutputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectEdgesToolbox(),
                Serdes.String().deserializer(), as.<EdgeValue>value().deserializer()
        );
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
        FileRemover.removeDir(this.stateDir);

    }

    @Test
    public void joinTest() {
        // Publish test input
        sendPsWithOb(1, 2);
        sendPsWithOb(1, 3);
        sendPsWithSub(1, 2);
        sendPsWithSub(1, 3);
        var res = edgesOutputTopic.readKeyValuesToMap();
        assertEquals(2, res.size());
        for (var v : res.values()) {
            assertNotNull(v.getSourceProjectEntity());
            assertNotNull(v.getTargetProjectEntity());
        }
    }

    @Test
    public void joinTestOtherWay() {
        // Publish test input
        sendPsWithSub(1, 2);
        sendPsWithSub(1, 3);
        sendPsWithOb(1, 2);
        sendPsWithOb(1, 3);

        var res = edgesOutputTopic.readKeyValuesToMap();
        assertEquals(2, res.size());
        for (var v : res.values()) {
            assertNotNull(v.getSourceProjectEntity());
            assertNotNull(v.getTargetProjectEntity());
        }
    }


    private void sendPsWithSub(int projectId, int statementId) {
        var kE = ProjectStatementKey.newBuilder().setStatementId(statementId).setProjectId(projectId).build();
        var vE = StatementWithSubValue.newBuilder()
                .setStatementId(0)
                .setProjectId(0)
                .setProjectCount(0)
                .setOrdNumOfDomain(0f)
                .setOrdNumOfRange(0f)
                .setSubjectId("0")
                .setPropertyId(0)
                .setObjectId("0")
                .setSubjectClassId(0)
                .setObjectClassId(0)
                .setSubjectLabel("0")
                .setObjectLabel("0")
                .setSubject(
                        NodeValue.newBuilder().setLabel("").setId("0").setClassId(0)
                                .setEntity(
                                        Entity.newBuilder()
                                                .setFkClass(0)
                                                .setCommunityVisibilityWebsite(false)
                                                .setCommunityVisibilityDataApi(false)
                                                .setCommunityVisibilityToolbox(false)
                                                .build())
                                .build()
                )
                .setObject(NodeValue.newBuilder().setLabel("").setId("0").setClassId(0)
                        .setEntity(
                                Entity.newBuilder()
                                        .setFkClass(0)
                                        .setCommunityVisibilityWebsite(true)
                                        .setCommunityVisibilityDataApi(true)
                                        .setCommunityVisibilityToolbox(true)
                                        .build())
                        .build())
                .setModifiedAt("")
                .setDeleted(false)
                .setSubjectEntityValue(
                        EntityValue.newBuilder()
                                .setProjectId(0)
                                .setEntityId("0")
                                .setClassId(0)
                                .setCommunityVisibilityToolbox(true)
                                .setCommunityVisibilityDataApi(true)
                                .setCommunityVisibilityWebsite(true)
                                .setProjectVisibilityDataApi(true)
                                .setProjectVisibilityWebsite(true)
                                .setDeleted(false)
                                .build()
                )
                .build();
        psWithSubInputTopic.pipeInput(kE, vE);
    }

    private void sendPsWithOb(int projectId, int statementId) {
        var kE = ProjectStatementKey.newBuilder().setStatementId(statementId).setProjectId(projectId).build();
        var vE = StatementWithObValue.newBuilder()
                .setStatementId(0)
                .setObjectEntityValue(
                        EntityValue.newBuilder()
                                .setProjectId(0)
                                .setEntityId("0")
                                .setClassId(0)
                                .setCommunityVisibilityToolbox(true)
                                .setCommunityVisibilityDataApi(true)
                                .setCommunityVisibilityWebsite(true)
                                .setProjectVisibilityDataApi(true)
                                .setProjectVisibilityWebsite(true)
                                .setDeleted(false)
                                .build()
                )
                .build();
        psWithObInputTopic.pipeInput(kE, vE);
    }
}
