package org.geovistory.toolbox.streams.project.items;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.kafka.streams.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityValue;
import org.geovistory.toolbox.streams.project.items.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.project.items.names.InputTopicNames;
import org.geovistory.toolbox.streams.project.items.names.OutputTopicNames;
import org.geovistory.toolbox.streams.testlib.FileRemover;
import org.geovistory.toolbox.streams.testlib.TopologyTestDriverProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ts.projects.info_proj_rel.Key;
import ts.projects.info_proj_rel.Value;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
public class ForkEntitiesTest {

    @Inject
    Topology topology;

    @Inject
    ConfiguredAvroSerde as;

    @Inject
    OutputTopicNames outputTopicNames;
    @Inject
    InputTopicNames inputTopicNames;
    @ConfigProperty(name = "kafka-streams.state.dir")
    public String stateDir;
    TopologyTestDriver testDriver;

    TestInputTopic<Key, Value> irpInputTopic;
    TestInputTopic<ts.information.resource.Key, ts.information.resource.Value> eInputTopic;
    TestOutputTopic<ProjectEntityKey, ProjectEntityValue> publicProjectEntities;
    TestOutputTopic<ProjectEntityKey, ProjectEntityValue> toolboxProjectEntities;
    TestOutputTopic<ProjectEntityKey, ProjectEntityValue> toolboxCommunityEntities;
    TestOutputTopic<ProjectEntityKey, ProjectEntityValue> publicCommunityEntities;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    public void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testApplicationId");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //as.schemaRegistryUrl = "mock://" + TestJoinProjectStatementsWithObject.class;

        testDriver = new TopologyTestDriver(topology, config);
        irpInputTopic = testDriver.createInputTopic(
                inputTopicNames.proInfProjRel(),
                as.kS(), as.vS()
        );
        eInputTopic = testDriver.createInputTopic(
                inputTopicNames.infResource(),
                as.kS(), as.vS()
        );

        publicProjectEntities = testDriver.createOutputTopic(
                outputTopicNames.publicProjectEntities(),
                as.kD(), as.vD()
        );

        toolboxProjectEntities = testDriver.createOutputTopic(
                outputTopicNames.toolboxProjectEntities(),
                as.kD(), as.vD()
        );
        toolboxCommunityEntities = testDriver.createOutputTopic(
                outputTopicNames.toolboxCommunityEntities(),
                as.kD(), as.vD()
        );
        publicCommunityEntities = testDriver.createOutputTopic(
                outputTopicNames.publicCommunityEntities(),
                as.kD(), as.vD()
        );
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
        FileRemover.removeDir(this.stateDir);
    }

    @Test
    public void testAddPublicEntity() {

        sendE(1, 1, true, true);
        sendIpr(1, 1, true, true);

        var tp = toolboxProjectEntities.readKeyValuesToMap();
        assertEquals(1, tp.size());

        var pp = publicProjectEntities.readKeyValuesToMap();
        assertEquals(1, pp.size());

        var tc = toolboxCommunityEntities.readKeyValuesToMap();
        assertEquals(1, tc.size());

        var pc = publicCommunityEntities.readKeyValuesToMap();
        assertEquals(1, pc.size());

    }


    @Test
    public void testCountUpAndDown() {

        sendE(1, 1, true, true);
        sendIpr(1, 1, true, true);
        sendIpr(1, 2, true, true);
        sendIpr(1, 1, false, true);
        sendIpr(1, 2, false, true);

        var tp = toolboxProjectEntities.readRecordsToList();
        var pp = publicProjectEntities.readRecordsToList();
        var tc = toolboxCommunityEntities.readRecordsToList();
        var pc = publicCommunityEntities.readRecordsToList();

        assertEquals(4, tp.size());
        assertEquals(false, tp.get(0).value().getDeleted$1());
        assertEquals(false, tp.get(1).value().getDeleted$1());
        assertEquals(true, tp.get(2).value().getDeleted$1());
        assertEquals(true, tp.get(3).value().getDeleted$1());

        assertEquals(4, pp.size());
        assertEquals(false, pp.get(0).value().getDeleted$1());
        assertEquals(false, pp.get(1).value().getDeleted$1());
        assertEquals(true, pp.get(2).value().getDeleted$1());
        assertEquals(true, pp.get(3).value().getDeleted$1());

        assertEquals(2, tc.size());
        assertEquals(false, tc.get(0).value().getDeleted$1());
        assertEquals(true, tc.get(1).value().getDeleted$1());

        assertEquals(2, pc.size());
        assertEquals(false, pc.get(0).value().getDeleted$1());
        assertEquals(true, pc.get(1).value().getDeleted$1());


    }

    @Test
    public void testPublishOnlyAllowed() {

        sendE(1, 1, false, false);
        sendIpr(1, 1, true, false);
        sendIpr(1, 2, true, false);

        var tp = toolboxProjectEntities.readRecordsToList();
        var pp = publicProjectEntities.readRecordsToList();
        var tc = toolboxCommunityEntities.readRecordsToList();
        var pc = publicCommunityEntities.readRecordsToList();

        assertEquals(2, tp.size());
        assertEquals(false, tp.get(0).value().getDeleted$1());
        assertEquals(false, tp.get(1).value().getDeleted$1());

        assertEquals(0, pp.size());

        assertEquals(0, tc.size());

        assertEquals(0, pc.size());

    }

    @Test
    public void testPublishOnlyPublicCommunity() {

        sendE(1, 1, false, true);
        sendIpr(1, 1, true, false);
        sendIpr(1, 2, true, false);

        var tp = toolboxProjectEntities.readRecordsToList();
        var pp = publicProjectEntities.readRecordsToList();
        var tc = toolboxCommunityEntities.readRecordsToList();
        var pc = publicCommunityEntities.readRecordsToList();

        assertEquals(2, tp.size());
        assertEquals(0, pp.size());
        assertEquals(0, tc.size());
        assertEquals(1, pc.size());
    }

    @Test
    public void testPublishOnlyToolboxCommunity() {

        sendE(1, 1, true, false);
        sendIpr(1, 1, true, false);
        sendIpr(1, 2, true, false);

        var tp = toolboxProjectEntities.readRecordsToList();
        var pp = publicProjectEntities.readRecordsToList();
        var tc = toolboxCommunityEntities.readRecordsToList();
        var pc = publicCommunityEntities.readRecordsToList();

        assertEquals(2, tp.size());
        assertEquals(0, pp.size());
        assertEquals(1, tc.size());
        assertEquals(0, pc.size());
    }

    @Test
    public void testPublishOnlyPublicProject() {

        sendE(1, 1, false, false);
        sendIpr(1, 1, true, true);
        sendIpr(1, 2, true, false);

        var tp = toolboxProjectEntities.readRecordsToList();
        var pp = publicProjectEntities.readRecordsToList();
        var tc = toolboxCommunityEntities.readRecordsToList();
        var pc = publicCommunityEntities.readRecordsToList();

        assertEquals(2, tp.size());
        assertEquals(1, pp.size());
        assertEquals(0, tc.size());
        assertEquals(0, pc.size());
    }


    private void sendIpr(int fkEntity, int fkProject, boolean inProject, Boolean projectPublic, Integer ordNumOfDomain, Integer ordNumOfRange) {
        var k = Key.newBuilder()
                .setFkEntity(fkEntity)
                .setFkProject(fkProject)
                .build();
        var v = Value.newBuilder()
                .setFkEntity(fkEntity)
                .setFkProject(fkProject)
                .setOrdNumOfDomain(ordNumOfDomain)
                .setOrdNumOfRange(ordNumOfRange)
                .setIsInProject(inProject)
                .setProjectVisibility("{ \"dataApi\": " + projectPublic.toString() + ", \"website\": true}")
                .setSchemaName("").setTableName("").setEntityVersion(1).build();
        irpInputTopic.pipeInput(k, v);
    }

    private void sendIpr(int fkEntity, int fkProject, boolean inProject, Boolean projectPublic) {
        sendIpr(fkEntity, fkProject, inProject, projectPublic, null, null);
    }


    private ts.information.resource.Value sendE(int pkEntity, int fkClass, Boolean communityToolbox, Boolean communityPublic) {
        var kE = ts.information.resource.Key.newBuilder().setPkEntity(pkEntity).build();
        var vE = ts.information.resource.Value.newBuilder()
                .setPkEntity(pkEntity).setFkClass(fkClass)
                .setCommunityVisibility("{ \"toolbox\": " + communityToolbox + ", \"dataApi\": " + communityPublic + ", \"website\": true}")
                .setSchemaName("").setTableName("").build();
        eInputTopic.pipeInput(kE, vE);
        return vE;
    }

}
