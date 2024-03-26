package org.geovistory.toolbox.streams.project.items;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.EntityValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.project.items.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.project.items.names.OutputTopicNames;
import org.geovistory.toolbox.streams.testlib.FileRemover;
import org.geovistory.toolbox.streams.testlib.TopologyTestDriverProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
public class TestJoinProjectStatementsWithObject {

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
    TestInputTopic<ProjectEntityKey, Integer> psByObInputTopic;
    TestOutputTopic<ProjectStatementKey, EntityValue> psWithObOutputTopic;


    @BeforeEach
    public void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testApplicationId");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //as.schemaRegistryUrl = "mock://" + TestJoinProjectStatementsWithObject.class;

        testDriver = new TopologyTestDriver(topology, config);
        peInputTopic = testDriver.createInputTopic(
                outputTopicNames.projectEntity(),
                as.<ProjectEntityKey>key().serializer(), as.<EntityValue>value().serializer()
        );
        psByObInputTopic = testDriver.createInputTopic(
                outputTopicNames.projectStatementByOb(),
                as.<ProjectEntityKey>key().serializer(), Serdes.Integer().serializer()
        );
        psWithObOutputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectStatementWithObByPk(),
                as.<ProjectStatementKey>key().deserializer(), as.<EntityValue>value().deserializer()
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
        sendPSbyOb(42, "i21", 52);
        sendPSbyOb(41, "i23", 53);
        sendPSbyOb(40, "i23", 53);

        var res = psWithObOutputTopic.readKeyValuesToMap();
        assertEquals(2, res.size());
    }

    @Test
    public void joinTestOtherWay() {
        sendPSbyOb(42, "i21", 52);
        sendPSbyOb(41, "i23", 53);
        sendPSbyOb(40, "i23", 53);
        sendPE(40, "i20");
        sendPE(43, "i20");
        sendPE(42, "i20");
        sendPE(41, "i20");
        sendPE(41, "i23");
        sendPE(40, "i23");

        var res = psWithObOutputTopic.readKeyValuesToMap();
        assertEquals(2, res.size());
    }


    private void sendPE(int projectId, String entityId) {
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(projectId).build();
        var vE = EntityValue.newBuilder()
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
        peInputTopic.pipeInput(kE, vE);
    }

    private void sendPSbyOb(int projectId, String entityId, int statementId) {
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(projectId).build();
        var vE = statementId;
        psByObInputTopic.pipeInput(kE, vE);
    }
}
