package org.geovistory.toolbox.streams.app;


import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaSerdeConfig;
import org.apache.kafka.streams.*;
import org.assertj.core.api.ListAssert;
import org.geovistory.toolbox.streams.avro.Id;
import org.geovistory.toolbox.streams.avro.ProfileIds;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.AvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.newArrayList;

@Testcontainers
class ProjectProfilesTopologyTest {

    // will be shared between test methods
    @Container
    private static final GenericContainer<?> APICURIO_CONTAINER = new GenericContainer<>(DockerImageName.parse("apicurio/apicurio-registry-mem:2.3.1.Final"))
            .withExposedPorts(8080);

    private TopologyTestDriver testDriver;
    private TestInputTopic<dev.projects.dfh_profile_proj_rel.Key, dev.projects.dfh_profile_proj_rel.Value> profileProjectTopic;
    private TestInputTopic<dev.system.config.Key, dev.system.config.Value> configTopic;
    private TestInputTopic<dev.projects.project.Key, dev.projects.project.Value> projectTopic;
    private TestOutputTopic<Id, ProfileIds> outputTopic;

    @BeforeEach
    void setup() {

        String address = APICURIO_CONTAINER.getHost();
        Integer port = APICURIO_CONTAINER.getFirstMappedPort();
        String apicurioRegistryUrl = "http://" + address + ":" + port + "/apis/registry/v2";
        AppConfig.INSTANCE.setApicurioRegistryUrl(apicurioRegistryUrl);
        System.out.println("apicurioRegistryUrl " + apicurioRegistryUrl);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        // URL for Apicurio Registry connection (including basic auth parameters)
        props.put(SerdeConfig.REGISTRY_URL, apicurioRegistryUrl);

        // Specify using specific (generated) Avro schema classes
        props.put(AvroKafkaSerdeConfig.USE_SPECIFIC_AVRO_READER, "true");

        Topology topology = ProjectProfilesTopology.build(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        projectTopic = testDriver.createInputTopic(
                ProjectProfilesTopology.input.TOPICS.project,
                AvroSerdes.ProProjectKey().serializer(),
                AvroSerdes.ProProjectValue().serializer());

        profileProjectTopic = testDriver.createInputTopic(
                ProjectProfilesTopology.input.TOPICS.dfh_profile_proj_rel,
                AvroSerdes.ProProfileProjRelKey().serializer(),
                AvroSerdes.ProProfileProjRelValue().serializer());

        configTopic = testDriver.createInputTopic(
                ProjectProfilesTopology.input.TOPICS.config,
                AvroSerdes.SysConfigKey().serializer(),
                AvroSerdes.SysConfigValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                ProjectProfilesTopology.output.TOPICS.projects_with_aggregated_profiles,
                AvroSerdes.IdKey().deserializer(),
                AvroSerdes.ProfileIdsValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testZeroProfiles() {
        var pKey = new dev.projects.project.Key(20);
        var pVal = dev.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

      /*  var cKey = new dev.system.config.Key(1);
        var cVal = dev.system.config.Value.newBuilder().setSchemaName("").setTableName("")
                .setKey("SYS_CONFIG")
                .setConfig("{\"ontome\": {\"requiredOntomeProfiles\": [5, 97]}}").build();
        configTopic.pipeInput(cKey, cVal);
*/
        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var projectId = new Id(20);
        var profileIds = outRecords.get(projectId).getProfileIds();
        assertThat(profileIds).hasSize(0);
    }

    @Test
    void testOneEnabledProfile() {
        var pKey = new dev.projects.project.Key(20);
        var pVal = dev.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

        var ppKey1 = new dev.projects.dfh_profile_proj_rel.Key(4);
        var ppVal1 = dev.projects.dfh_profile_proj_rel.Value.newBuilder()
                .setFkProject(20).setFkProfile(100).setEnabled(true).build();
        profileProjectTopic.pipeInput(ppKey1, ppVal1);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var projectId = new Id(20);
        var profileIds = outRecords.get(projectId).getProfileIds();
        assertThat(profileIds).hasSize(1);
        assertThat(profileIds.get(0)).isEqualTo(100);
    }

    @Test
    void testOneRequiredProfile() {
        var pKey = new dev.projects.project.Key(20);
        var pVal = dev.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

        var cKey = new dev.system.config.Key(1);
        var cVal = dev.system.config.Value.newBuilder().setSchemaName("").setTableName("")
                .setKey("SYS_CONFIG")
                .setConfig("{\"ontome\": {\"requiredOntomeProfiles\": [5]}}").build();
        configTopic.pipeInput(cKey, cVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var projectId = new Id(20);
        var profileIds = outRecords.get(projectId).getProfileIds();
        assertThat(profileIds).hasSize(1);
        assertThat(profileIds.get(0)).isEqualTo(5);
    }

    @Test
    void testTwoEnabledAndTwoRequiredProfiles() {
        var pKey = new dev.projects.project.Key(20);
        var pVal = dev.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

        var ppKey = new dev.projects.dfh_profile_proj_rel.Key(4);
        var ppVal = dev.projects.dfh_profile_proj_rel.Value.newBuilder()
                .setFkProject(20).setFkProfile(100).setEnabled(true).build();
        profileProjectTopic.pipeInput(ppKey, ppVal);

        ppKey.setPkEntity(2);
        ppVal.setFkProfile(101);
        profileProjectTopic.pipeInput(ppKey, ppVal);

        var cKey = new dev.system.config.Key(1);
        var cVal = dev.system.config.Value.newBuilder().setSchemaName("").setTableName("")
                .setKey("SYS_CONFIG")
                .setConfig("{\"ontome\": {\"requiredOntomeProfiles\": [5, 97]}}").build();
        configTopic.pipeInput(cKey, cVal);

        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var projectId = new Id(20);
        var profileIds = outRecords.get(projectId).getProfileIds();
        assertThat(profileIds).hasSize(4);
        var expected = newArrayList(100, 101, 5, 97);
        assertThat(profileIds).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void testRemoveEnabledProfile() {
        var pKey = new dev.projects.project.Key(20);
        var pVal = dev.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

        var ppKey = new dev.projects.dfh_profile_proj_rel.Key(1);
        var ppVal = dev.projects.dfh_profile_proj_rel.Value.newBuilder()
                .setFkProject(20).setFkProfile(100).setEnabled(true).build();
        profileProjectTopic.pipeInput(ppKey, ppVal);

        ppKey.setPkEntity(2);
        ppVal.setFkProfile(101);
        profileProjectTopic.pipeInput(ppKey, ppVal);

        // add remove first record (tombstone)
        ppKey.setPkEntity(1);
        ppVal = null;
        profileProjectTopic.pipeInput(ppKey, ppVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var projectId = new Id(20);
        var profileIds = outRecords.get(projectId).getProfileIds();
        assertThat(profileIds).hasSize(1);
        assertThat(profileIds.get(0)).isEqualTo(101);
    }

    @Test
    void testDisableEnabledProfile() {
        var pKey = new dev.projects.project.Key(20);
        var pVal = dev.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

        var ppKey = new dev.projects.dfh_profile_proj_rel.Key(1);
        var ppVal = dev.projects.dfh_profile_proj_rel.Value.newBuilder()
                .setFkProject(20).setFkProfile(100).setEnabled(true).build();
        profileProjectTopic.pipeInput(ppKey, ppVal);

        ppKey.setPkEntity(1);
        ppVal.setEnabled(false);
        profileProjectTopic.pipeInput(ppKey, ppVal);

        var outRecords = outputTopic.readKeyValuesToMap();
        var profileIds = outRecords.get(new Id(20)).getProfileIds();
        assertThat(profileIds).hasSize(0);
    }

    @Test
    void testRemoveRequiredProfile() {
        var pKey = new dev.projects.project.Key(20);
        var pVal = dev.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

        var cKey = new dev.system.config.Key(1);
        var cVal = dev.system.config.Value.newBuilder().setSchemaName("").setTableName("")
                .setKey("SYS_CONFIG")
                .setConfig("{\"ontome\": {\"requiredOntomeProfiles\": [5]}}").build();
        configTopic.pipeInput(cKey, cVal);

        cVal.setConfig("{\"ontome\": {\"requiredOntomeProfiles\": []}}");
        configTopic.pipeInput(cKey, cVal);

        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var profileIds = outRecords.get(new Id(20)).getProfileIds();
        assertThat(profileIds).hasSize(0);
    }


    @Test
    void testRemoveProjectTombstone() {

        var pKey = new dev.projects.project.Key(20);
        var pVal = dev.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

        var ppKey = new dev.projects.dfh_profile_proj_rel.Key(4);
        var ppVal = dev.projects.dfh_profile_proj_rel.Value.newBuilder()
                .setFkProject(20).setFkProfile(100).setEnabled(true).build();
        profileProjectTopic.pipeInput(ppKey, ppVal);

        projectTopic.pipeInput(pKey, null);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        assertThat(outRecords.get(new Id(20))).isEqualTo(null);
    }


}
