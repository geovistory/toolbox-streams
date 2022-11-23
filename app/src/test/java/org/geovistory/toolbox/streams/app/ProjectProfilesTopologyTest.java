package org.geovistory.toolbox.streams.app;


import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaSerdeConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.Dummy;
import org.geovistory.toolbox.streams.avro.ProjectProfileId;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.AvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class ProjectProfilesTopologyTest {

    // will be shared between test methods
    @Container
    private static final GenericContainer<?> APICURIO_CONTAINER = new GenericContainer<>(DockerImageName.parse("apicurio/apicurio-registry-mem:2.3.1.Final"))
            .withExposedPorts(8080);

    private TopologyTestDriver testDriver;
    private TestInputTopic<dev.projects.dfh_profile_proj_rel.Key, dev.projects.dfh_profile_proj_rel.Value> profileProjectTopic;
    private TestInputTopic<Integer, Integer> requiredProfilesTopic;
    private TestInputTopic<dev.projects.project.Key, dev.projects.project.Value> projectTopic;
    private TestOutputTopic<ProjectProfileId, Dummy> outputTopic;

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

        requiredProfilesTopic = testDriver.createInputTopic(
                ProjectProfilesTopology.input.TOPICS.required_profiles,
                Serdes.Integer().serializer(),
                Serdes.Integer().serializer());

        outputTopic = testDriver.createOutputTopic(
                ProjectProfilesTopology.output.TOPICS.projects_with_aggregated_profiles,
                AvroSerdes.ProjectProfileIdKey().deserializer(),
                AvroSerdes.DummyValue().deserializer());
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


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(0);

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
        var id = new ProjectProfileId(20, 100);
        assertThat(outRecords.containsKey(id)).isTrue();
    }

    @Test
    void testOneRequiredProfile() {
        var pKey = new dev.projects.project.Key(20);
        var pVal = dev.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

        requiredProfilesTopic.pipeInput(5, 5);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var id = new ProjectProfileId(20, 5);
        assertThat(outRecords.containsKey(id)).isTrue();
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

        requiredProfilesTopic.pipeInput(5, 5);
        requiredProfilesTopic.pipeInput(97, 97);

        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(4);
        assertThat(outRecords.containsKey(new ProjectProfileId(20, 100))).isTrue();
        assertThat(outRecords.containsKey(new ProjectProfileId(20, 101))).isTrue();
        assertThat(outRecords.containsKey(new ProjectProfileId(20, 5))).isTrue();
        assertThat(outRecords.containsKey(new ProjectProfileId(20, 97))).isTrue();
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

        // add remove first record
        ppKey.setPkEntity(1);
        ppVal = null;
        profileProjectTopic.pipeInput(ppKey, ppVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        assertThat(outRecords.containsKey(new ProjectProfileId(20, 100))).isFalse();
        assertThat(outRecords.containsKey(new ProjectProfileId(20, 101))).isTrue();
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
        assertThat(outRecords).hasSize(0);
    }

    @Test
    void testRemoveRequiredProfile() {
        var pKey = new dev.projects.project.Key(20);
        var pVal = dev.projects.project.Value.newBuilder().build();

        projectTopic.pipeInput(pKey, pVal);

        requiredProfilesTopic.pipeInput(5, 5);

        Integer n = null;
        requiredProfilesTopic.pipeInput(5, n);

        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(0);
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
        assertThat(outRecords).hasSize(0);
    }


}
