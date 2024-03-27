package org.geovistory.toolbox.streams.project.items;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.jsonmodels.CommunityVisibility;
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
import static org.junit.jupiter.api.Assertions.assertNull;

@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
public class ForkEdgesTest {

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
    TestInputTopic<ts.information.statement.Key, StatementEnrichedValue> sweInputTopic;
    TestInputTopic<ts.information.statement.Key, StatementEnrichedValue> swlInputTopic;
    TestInputTopic<ts.information.resource.Key, ts.information.resource.Value> eInputTopic;
    TestOutputTopic<String, EdgeValue> projectEdgesPublic;
    TestOutputTopic<String, EdgeValue> projectEdgesToolbox;
    TestOutputTopic<String, EdgeValue> communityEdgesToolbox;
    TestOutputTopic<String, EdgeValue> communityEdgesPublic;
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
        sweInputTopic = testDriver.createInputTopic(
                inputTopicNames.getStatementWithEntity(),
                as.kS(), as.vS()
        );
        swlInputTopic = testDriver.createInputTopic(
                inputTopicNames.getStatementWithLiteral(),
                as.kS(), as.vS()
        );
        eInputTopic = testDriver.createInputTopic(
                inputTopicNames.infResource(),
                as.kS(), as.vS()
        );

        projectEdgesPublic = testDriver.createOutputTopic(
                outputTopicNames.projectEdgesPublic(),
                Serdes.String().deserializer(), as.vD()
        );

        projectEdgesToolbox = testDriver.createOutputTopic(
                outputTopicNames.projectEdgesToolbox(),
                Serdes.String().deserializer(), as.vD()
        );
        communityEdgesToolbox = testDriver.createOutputTopic(
                outputTopicNames.communityEdgesToolbox(),
                Serdes.String().deserializer(), as.vD()
        );
        communityEdgesPublic = testDriver.createOutputTopic(
                outputTopicNames.communityEdgesPublic(),
                Serdes.String().deserializer(), as.vD()
        );
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
        FileRemover.removeDir(this.stateDir);
    }

    @Test
    public void testAddPublicEdge() {

        var e1 = sendE(1, 1, true, true);
        sendIpr(1, 1, true, true);
        sendSWithLiteral(2, 1, e1, "i99");
        sendIpr(2, 1, true, true);

        var ptbx = projectEdgesToolbox.readKeyValuesToMap();
        assertEquals(1, ptbx.size());

        var ppub = projectEdgesPublic.readKeyValuesToMap();
        assertEquals(1, ppub.size());

        var ctbx = communityEdgesToolbox.readKeyValuesToMap();
        assertEquals(1, ctbx.size());

        var cpub = communityEdgesPublic.readKeyValuesToMap();
        assertEquals(1, cpub.size());

    }

    @Test
    public void testAddPrivateEdge() {

        var e1 = sendE(1, 1, false, false);
        sendIpr(1, 1, true, false);
        sendSWithLiteral(2, 1, e1, "i99");
        sendIpr(2, 1, true, true);

        var ptbx = projectEdgesToolbox.readKeyValuesToMap();
        assertEquals(1, ptbx.size());

        var ppub = projectEdgesPublic.readKeyValuesToMap();
        assertEquals(0, ppub.size());

        var ctbx = communityEdgesToolbox.readKeyValuesToMap();
        assertEquals(0, ctbx.size());

        var cpub = communityEdgesPublic.readKeyValuesToMap();
        assertEquals(0, cpub.size());

    }

    @Test
    public void testTurnPublicSubjectPrivate() {

        var e1 = sendE(1, 1, true, true);
        sendIpr(1, 1, true, true);
        sendSWithLiteral(2, 1, e1, "i99");
        sendIpr(2, 1, true, true);

        var ptbx = projectEdgesToolbox.readKeyValuesToMap();
        assertEquals(1, ptbx.size());

        var ppub = projectEdgesPublic.readKeyValuesToMap();
        assertEquals(1, ppub.size());

        var ctbx = communityEdgesToolbox.readKeyValuesToMap();
        assertEquals(1, ctbx.size());

        var cpub = communityEdgesPublic.readKeyValuesToMap();
        assertEquals(1, cpub.size());

        sendIpr(1, 1, true, false);

        var ppubList = projectEdgesPublic.readRecordsToList();
        assertEquals(true, ppubList.get(0).value().getDeleted());

        e1 = sendE(1, 1, false, true);
        var ctbxList = communityEdgesToolbox.readRecordsToList();
        sendSWithLiteral(2, 1, e1, "i99");

        ctbxList = communityEdgesToolbox.readRecordsToList();
        assertEquals(true, ctbxList.get(0).value().getDeleted());

        e1 = sendE(1, 1, false, false);
        var cpubList = communityEdgesPublic.readRecordsToList();
        sendSWithLiteral(2, 1, e1, "i99");
        cpubList = communityEdgesPublic.readRecordsToList();
        assertEquals(true, cpubList.get(0).value().getDeleted());
        assertEquals(0, cpubList.get(0).value().getProjectCount());

    }

    @Test
    public void testTurnPrivateSubjectPublic() {

        var e1 = sendE(1, 1, false, false);
        sendIpr(1, 1, true, false);
        sendSWithLiteral(2, 1, e1, "i99");
        sendIpr(2, 1, true, true);

        var ptbx = projectEdgesToolbox.readKeyValuesToMap();
        assertEquals(1, ptbx.size());

        var ppub = projectEdgesPublic.readRecordsToList();
        assertEquals(0, ppub.size());

        var ctbx = communityEdgesToolbox.readKeyValuesToMap();
        assertEquals(0, ctbx.size());

        var cpub = communityEdgesPublic.readKeyValuesToMap();
        assertEquals(0, cpub.size());

        sendIpr(1, 1, true, true);

        ppub = projectEdgesPublic.readRecordsToList();
        assertEquals(false, ppub.get(0).value().getDeleted());


        e1 = sendE(1, 1, true, true);
        sendSWithLiteral(2, 1, e1, "i99");
        var ctbxList = communityEdgesToolbox.readRecordsToList();
        assertEquals(1, ctbxList.get(0).value().getProjectCount());

    }

    @Test
    public void testCountUp() {

        var e1 = sendE(1, 1, true, false);
        sendSWithLiteral(2, 1, e1, "i99");

        sendIpr(1, 1, true, false);
        sendIpr(2, 1, true, true);
        sendIpr(2, 2, true, true);
        sendIpr(2, 3, true, true);
        sendIpr(2, 4, true, true);

        var ctbx = communityEdgesToolbox.readKeyValuesToMap();
        assertEquals(4, ctbx.get("toolbox_i1_1_true_i99").getProjectCount());

    }

    @Test
    public void testCountDownAndDeletion() {


        var e1 = sendE(1, 1, true, false);
        sendSWithLiteral(2, 1, e1, "i99");

        sendIpr(1, 1, true, false);
        sendIpr(2, 1, true, true);
        sendIpr(2, 2, true, true);
        sendIpr(2, 3, true, true);
        sendIpr(2, 4, true, true);
        var ctbx = communityEdgesToolbox.readKeyValuesToMap();
        assertEquals(4, ctbx.get("toolbox_i1_1_true_i99").getProjectCount());

        // remove from project 2
        sendIpr(2, 2, false, false);
        ctbx = communityEdgesToolbox.readKeyValuesToMap();
        assertEquals(3, ctbx.get("toolbox_i1_1_true_i99").getProjectCount());
        assertEquals(false, ctbx.get("toolbox_i1_1_true_i99").getDeleted());

        // remove from community toolbox
        e1 = sendE(1, 1, false, false);
        sendSWithLiteral(2, 1, e1, "i99");
        ctbx = communityEdgesToolbox.readKeyValuesToMap();
        assertEquals(0, ctbx.get("toolbox_i1_1_true_i99").getProjectCount());
        assertEquals(true, ctbx.get("toolbox_i1_1_true_i99").getDeleted());
    }


    @Test
    public void testOrdNumAverage() {


        var e1 = sendE(1, 1, true, false);
        sendSWithLiteral(2, 1, e1, "i99");

        sendIpr(1, 1, true, true);
        sendIpr(2, 1, true, true, null, 1);
        sendIpr(2, 2, true, true, null, 4);
        var ctbx = communityEdgesToolbox.readKeyValuesToMap();
        assertEquals(2.5f, ctbx.get("toolbox_i1_1_true_i99").getOrdNum());

        sendIpr(2, 3, true, true, null, 1);
        ctbx = communityEdgesToolbox.readKeyValuesToMap();
        assertEquals(2f, ctbx.get("toolbox_i1_1_true_i99").getOrdNum());

        // remove from community toolbox
        e1 = sendE(1, 1, false, false);
        sendSWithLiteral(2, 1, e1, "i99");
        ctbx = communityEdgesToolbox.readKeyValuesToMap();
        assertNull(ctbx.get("toolbox_i1_1_true_i99").getOrdNum());
        assertEquals(true, ctbx.get("toolbox_i1_1_true_i99").getDeleted());
    }

    @Test
    public void testOrdNumAverageIsNull() {


        var e1 = sendE(1, 1, true, false);
        sendSWithLiteral(2, 1, e1, "i99");

        sendIpr(1, 1, true, true);
        sendIpr(2, 1, true, true, null, null);
        var ctbx = communityEdgesToolbox.readKeyValuesToMap();
        assertEquals(null, ctbx.get("toolbox_i1_1_true_i99").getOrdNum());

    }

    private void sendIpr(int fkEntity, int fkProject, boolean inProject, Boolean projectPublic, Integer ordNumOfDomain, Integer ordNumOfRange) {
        var k = ts.projects.info_proj_rel.Key.newBuilder()
                .setFkEntity(fkEntity)
                .setFkProject(fkProject)
                .build();
        var v = ts.projects.info_proj_rel.Value.newBuilder()
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

    private void sendSWithLiteral(int pkEntity, int propertyId, ts.information.resource.Value subject, String objectId) {

        var kE = ts.information.statement.Key.newBuilder().setPkEntity(pkEntity).build();
        var vE = StatementEnrichedValue.newBuilder()
                .setSubjectId("i" + subject.getPkEntity())
                .setSubject(NodeValue.newBuilder()
                        .setEntity(tranformEntity(subject))
                        .setClassId(5)
                        .build())
                .setPropertyId(propertyId)
                .setObjectId(objectId)
                .setObject(NodeValue.newBuilder()
                        .setAppellation(Appellation.newBuilder().setString("A").setFkClass(6).setPkEntity(0).build())
                        .setLabel("A")
                        .setClassId(6)
                        .build())
                .build();
        swlInputTopic.pipeInput(kE, vE);
    }

    private void sendSWithEntity(int pkEntity, int propertyId, String subjectId, String objectId) {
        var kE = ts.information.statement.Key.newBuilder().setPkEntity(pkEntity).build();
        var vE = StatementEnrichedValue.newBuilder()
                .setSubjectId(subjectId)
                .setSubject(NodeValue.newBuilder()
                        .setEntity(Entity.newBuilder()
                                .setCommunityVisibilityToolbox(true)
                                .setCommunityVisibilityWebsite(true)
                                .setCommunityVisibilityDataApi(true)
                                .setFkClass(5).setPkEntity(0).build())
                        .setLabel("A")
                        .setClassId(5)
                        .build())
                .setPropertyId(propertyId)
                .setObjectId(objectId)
                .setObject(NodeValue.newBuilder()
                        .setEntity(Entity.newBuilder()
                                .setCommunityVisibilityToolbox(true)
                                .setCommunityVisibilityWebsite(true)
                                .setCommunityVisibilityDataApi(true)
                                .setFkClass(5).setPkEntity(0).build())
                        .setLabel("A")
                        .setClassId(5)
                        .build())
                .build();
        sweInputTopic.pipeInput(kE, vE);
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

    /**
     * @param infEntity the value from the database
     * @return a projected, more lightweight, value
     */
    private Entity tranformEntity(ts.information.resource.Value infEntity) {
        var communityCanSeeInToolbox = false;
        var communityCanSeeInDataApi = false;
        var communityCanSeeInWebsite = false;
        if (infEntity.getCommunityVisibility() != null) {

            try {
                var communitVisibility = mapper.readValue(infEntity.getCommunityVisibility(), CommunityVisibility.class);
                if (communitVisibility.toolbox) communityCanSeeInToolbox = true;
                if (communitVisibility.dataApi) communityCanSeeInDataApi = true;
                if (communitVisibility.website) communityCanSeeInWebsite = true;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return Entity.newBuilder()
                .setPkEntity(infEntity.getPkEntity())
                .setFkClass(infEntity.getFkClass())
                .setCommunityVisibilityToolbox(communityCanSeeInToolbox)
                .setCommunityVisibilityDataApi(communityCanSeeInDataApi)
                .setCommunityVisibilityWebsite(communityCanSeeInWebsite)
                .build();
    }

}
