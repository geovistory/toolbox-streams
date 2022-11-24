package org.geovistory.toolbox.streams.app;


import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaSerdeConfig;
import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.Id;
import org.geovistory.toolbox.streams.avro.ProfileIds;
import org.geovistory.toolbox.streams.avro.ProjectPropertyKey;
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
class ProjectPropertyTopologyTest {

    // will be shared between test methods
    @Container
    private static final GenericContainer<?> APICURIO_CONTAINER = new GenericContainer<>(DockerImageName.parse("apicurio/apicurio-registry-mem:2.3.1.Final"))
            .withExposedPorts(8080);

    private TopologyTestDriver testDriver;
    private TestInputTopic<dev.data_for_history.api_property.Key, dev.data_for_history.api_property.Value> apiPropertyTopic;
    private TestInputTopic<Id, ProfileIds> projectProfilesTopic;
    private TestOutputTopic<ProjectPropertyKey, ProjectPropertyKey> outputTopic;

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

        apiPropertyTopic = testDriver.createInputTopic(
                ProjectProfilesTopology.input.TOPICS.dfh_profile_proj_rel,
                AvroSerdes.DfhApiPropertyKey().serializer(),
                AvroSerdes.DfhApiPropertyValue().serializer());

        projectProfilesTopic = testDriver.createInputTopic(
                ProjectProfilesTopology.input.TOPICS.config,
                AvroSerdes.IdKey().serializer(),
                AvroSerdes.ProfileIdsValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                ProjectProfilesTopology.output.TOPICS.projects_with_aggregated_profiles,
                AvroSerdes.ProjectPropertyKeyKey().deserializer(),
                AvroSerdes.ProjectPropertyKeyValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testOneApiProperty() {
        // add profile
        var pKey = new Id(1);
        var pVal = ProfileIds.newBuilder().build();
        pVal.getProfileIds().add(97);
        projectProfilesTopic.pipeInput(pKey, pVal);

        var apKey = new dev.data_for_history.api_property.Key(1);
        var apVal = dev.data_for_history.api_property.Value.newBuilder()
                .setDfhFkProfile(97)
                .setDfhPropertyDomain(33)
                .setDfhPkProperty(44)
                .setDfhPropertyRange(55)
                .build();
        apiPropertyTopic.pipeInput(apKey, apVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var projectPropertyKey = ProjectPropertyKey.newBuilder().
                setProjectId(1)
                .setDomainId(33)
                .setPropertyId(44)
                .setRangeId(55)
                .build();
        assertThat(outRecords.containsKey(projectPropertyKey)).isTrue();
    }


}
