package org.geovistory.toolbox.streams.project.items;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import lib.FileRemover;
import org.apache.kafka.streams.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.project.items.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.project.items.names.OutputTopicNames;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class TestJoinProjectStatementsWithSubject {

    @Inject
    Topology topology;

    @Inject
    ConfiguredAvroSerde as;

    @Inject
    OutputTopicNames outputTopicNames;
    @ConfigProperty(name = "kafka-streams.state.dir")
    public String stateDir;

    TopologyTestDriver testDriver;
    TestInputTopic<ProjectEntityKey, EntityValue> peInputTopic;
    TestInputTopic<ProjectEntityKey, StatementValue> psBySubInputTopic;
    TestOutputTopic<ProjectStatementKey, StatementWithSubValue> psWithSubOutputTopic;


    @BeforeEach
    public void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testApplicationId");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        as.schemaRegistryUrl = "mock://" + TestJoinProjectStatementsWithSubject.class;

        testDriver = new TopologyTestDriver(topology, config);
        peInputTopic = testDriver.createInputTopic(
                outputTopicNames.projectEntity(),
                as.<ProjectEntityKey>key().serializer(), as.<EntityValue>value().serializer()
        );
        psBySubInputTopic = testDriver.createInputTopic(
                outputTopicNames.projectStatementBySub(),
                as.<ProjectEntityKey>key().serializer(), as.<StatementValue>value().serializer()
        );
        psWithSubOutputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectStatementWithSubByPk(),
                as.<ProjectStatementKey>key().deserializer(), as.<StatementWithSubValue>value().deserializer()
        );
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
        FileRemover.removeDir(this.stateDir);
    }

    @Test
    public void joinTest() {
        // Publish test input numbers
        sendPE(40, "i20");
        sendPE(43, "i20");
        sendPE(42, "i20");
        sendPE(41, "i20");
        sendPE(41, "i23");
        sendPE(40, "i23");
        sendPSbySub(42, "i21", 52);
        sendPSbySub(41, "i23", 53);
        sendPSbySub(40, "i23", 53);

        var res = psWithSubOutputTopic.readKeyValuesToMap();
        assertEquals(3, res.size());

    }

    @Test
    public void joinTestOtherWay() {
        sendPSbySub(42, "i21", 52);
        sendPSbySub(41, "i23", 53);
        sendPSbySub(40, "i23", 53);
        sendPE(40, "i20");
        sendPE(43, "i20");
        sendPE(42, "i20");
        sendPE(41, "i20");
        sendPE(41, "i23");
        sendPE(40, "i23");

        var res = psWithSubOutputTopic.readKeyValuesToMap();
        assertEquals(3, res.size());
        // subject entity value should be null because "i21" is not in sendPE
        assertNull(
                res.get(ProjectStatementKey.newBuilder().setProjectId(42).setStatementId(52).build())
                        .getSubjectEntityValue()
        );
        // subject entity value should not be null
        assertNotNull(
                res.get(ProjectStatementKey.newBuilder().setProjectId(41).setStatementId(53).build())
                        .getSubjectEntityValue()
        );
        // subject entity value should not be null
        assertNotNull(
                res.get(ProjectStatementKey.newBuilder().setProjectId(40).setStatementId(53).build())
                        .getSubjectEntityValue()
        );

    }


    private ProjectEntityKey sendPE(int projectId, String entityId) {
        var k = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(projectId).build();
        var v = EntityValue.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .setClassId(1)
                .setCommunityVisibilityToolbox(true)
                .setCommunityVisibilityDataApi(true)
                .setCommunityVisibilityWebsite(true)
                .setProjectVisibilityDataApi(true)
                .setProjectVisibilityWebsite(true)
                .setDeleted(false)
                .build();
        peInputTopic.pipeInput(k, v);
        return k;
    }

    private ProjectEntityKey sendPSbySub(int projectId, String subjectId, int statementId) {
        var k = ProjectEntityKey.newBuilder().setEntityId(subjectId).setProjectId(projectId).build();
        var v = StatementValue.newBuilder()
                .setStatementId(statementId)
                .setProjectId(projectId)
                .setProjectCount(1)
                .setOrdNumOfDomain(1f)
                .setOrdNumOfRange(1f)
                .setSubjectId(subjectId)
                .setPropertyId(1)
                .setObjectId("1")
                .setSubjectClassId(1)
                .setObjectClassId(1)
                .setSubjectLabel("")
                .setObjectLabel("")
                .setSubject(null)
                .setObject(NodeValue.newBuilder()
                        .setClassId(0)
                        .setEntity(Entity.newBuilder()
                                .setCommunityVisibilityToolbox(true)
                                .setCommunityVisibilityWebsite(true)
                                .setCommunityVisibilityDataApi(true)
                                .setFkClass(5).setPkEntity(0).build()).build()
                )
                .setDeleted(false).build();
        psBySubInputTopic.pipeInput(k, v);
        return k;
    }
}
