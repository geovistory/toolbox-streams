package org.geovistory.toolbox.streams.app;


import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.classic.methods.HttpGet;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.classic.methods.HttpUriRequest;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.HttpStatus;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaSerdeConfig;
import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.AvroSerdes;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class ProjectPropertyTopologyTest {

    // will be shared between test methods
    @Container
    private static final GenericContainer<?> APICURIO_CONTAINER = new GenericContainer<>(DockerImageName.parse("apicurio/apicurio-registry-mem:2.3.1.Final"))
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/"));


    private TopologyTestDriver testDriver;
    private TestInputTopic<dev.data_for_history.api_property.Key, dev.data_for_history.api_property.Value> apiPropertyTopic;
    private TestInputTopic<ProjectProfileKey, ProjectProfileValue> projectProfilesTopic;
    private TestOutputTopic<ProjectPropertyKey, ProjectPropertyValue> outputTopic;

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
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");

        // URL for Apicurio Registry connection (including basic auth parameters)
        props.put(SerdeConfig.REGISTRY_URL, apicurioRegistryUrl);

        // Specify using specific (generated) Avro schema classes
        props.put(AvroKafkaSerdeConfig.USE_SPECIFIC_AVRO_READER, "true");

        Topology topology = ProjectPropertyTopology.build(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new AvroSerdes();

        apiPropertyTopic = testDriver.createInputTopic(
                ProjectPropertyTopology.input.TOPICS.api_property,
                avroSerdes.DfhApiPropertyKey().serializer(),
                avroSerdes.DfhApiPropertyValue().serializer());

        projectProfilesTopic = testDriver.createInputTopic(
                ProjectPropertyTopology.input.TOPICS.project_profile,
                avroSerdes.ProjectProfileKey().serializer(),
                avroSerdes.ProjectProfileValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                ProjectPropertyTopology.output.TOPICS.project_property,
                avroSerdes.ProjectPropertyKey().deserializer(),
                avroSerdes.ProjectPropertyValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testOneProjectProfileAndOneApiProperty() {
        // add project profile rel
        var pKey = ProjectProfileKey.newBuilder()
                .setProjectId(1)
                .setProfileId(97)
                .build();
        var pVal = ProjectProfileValue.newBuilder()
                .setProjectId(1)
                .setProfileId(97)
                .setDeleted$1(false)
                .build();
        projectProfilesTopic.pipeInput(pKey, pVal);

        // add property
        var apKey = new dev.data_for_history.api_property.Key(1);
        var apVal = dev.data_for_history.api_property.Value.newBuilder()
                .setDfhAncestorProperties(new ArrayList<>())
                .setDfhParentProperties(new ArrayList<>())
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


    @Test
    void testTwoProjectsProfilesAndTwoApiProperty() {
        // add first project profile rel
        var pKey = ProjectProfileKey.newBuilder()
                .setProjectId(1)
                .setProfileId(97)
                .build();
        var pVal = ProjectProfileValue.newBuilder()
                .setProjectId(1)
                .setProfileId(97)
                .setDeleted$1(false)
                .build();
        projectProfilesTopic.pipeInput(pKey, pVal);

        // add second project profile rel
        pKey.setProfileId(98);
        pVal.setProfileId(98);
        projectProfilesTopic.pipeInput(pKey, pVal);

        // add first property
        var apKey = new dev.data_for_history.api_property.Key(1);
        var apVal = dev.data_for_history.api_property.Value.newBuilder()
                .setDfhAncestorProperties(new ArrayList<>())
                .setDfhParentProperties(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPropertyDomain(33)
                .setDfhPkProperty(44)
                .setDfhPropertyRange(55)
                .build();
        apiPropertyTopic.pipeInput(apKey, apVal);

        // add second property
        apKey.setPkEntity(2);
        apVal.setDfhPkProperty(45);
        apiPropertyTopic.pipeInput(apKey, apVal);
        // add third property
        apVal.setDfhFkProfile(98);
        apVal.setDfhPkProperty(46);
        apiPropertyTopic.pipeInput(apKey, apVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(3);
        var projectPropertyKey = ProjectPropertyKey.newBuilder().
                setProjectId(1)
                .setDomainId(33)
                .setPropertyId(44)
                .setRangeId(55)
                .build();
        assertThat(outRecords.containsKey(projectPropertyKey)).isTrue();
        projectPropertyKey.setPropertyId(45);
        assertThat(outRecords.containsKey(projectPropertyKey)).isTrue();
        projectPropertyKey.setPropertyId(46);
        assertThat(outRecords.containsKey(projectPropertyKey)).isTrue();
    }

    @Test
    void testMarkOneProjectAsDeleted() {
        // add project profile rel
        var pKey = ProjectProfileKey.newBuilder()
                .setProjectId(1)
                .setProfileId(97)
                .build();
        var pVal = ProjectProfileValue.newBuilder()
                .setProjectId(1)
                .setProfileId(97)
                .setDeleted$1(false)
                .build();
        projectProfilesTopic.pipeInput(pKey, pVal);

        // add property
        var apKey = new dev.data_for_history.api_property.Key(1);
        var apVal = dev.data_for_history.api_property.Value.newBuilder()
                .setDfhAncestorProperties(new ArrayList<>())
                .setDfhParentProperties(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPropertyDomain(33)
                .setDfhPkProperty(44)
                .setDfhPropertyRange(55)
                .build();
        apiPropertyTopic.pipeInput(apKey, apVal);

        // mark project profile as deleted
        pVal.setDeleted$1(true);
        projectProfilesTopic.pipeInput(pKey, pVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        var projectPropertyKey = ProjectPropertyKey.newBuilder().
                setProjectId(1)
                .setDomainId(33)
                .setPropertyId(44)
                .setRangeId(55)
                .build();
        assertThat(outRecords.get(projectPropertyKey).getDeleted$1()).isTrue();
    }


    @Test
    void testMarkOnePropertyAsDeleted() {
        // add project profile rel
        var pKey = ProjectProfileKey.newBuilder()
                .setProjectId(1)
                .setProfileId(97)
                .build();
        var pVal = ProjectProfileValue.newBuilder()
                .setProjectId(1)
                .setProfileId(97)
                .setDeleted$1(false)
                .build();
        projectProfilesTopic.pipeInput(pKey, pVal);

        // add property
        var apKey = new dev.data_for_history.api_property.Key(1);
        var apVal = dev.data_for_history.api_property.Value.newBuilder()
                .setDfhAncestorProperties(new ArrayList<>())
                .setDfhParentProperties(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPropertyDomain(33)
                .setDfhPkProperty(44)
                .setDfhPropertyRange(55)
                .build();
        apiPropertyTopic.pipeInput(apKey, apVal);

        // mark property as deleted
        apVal.setDeleted$1("true");
        apiPropertyTopic.pipeInput(apKey, apVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readRecordsToList();
        assertThat(outRecords).hasSize(3);
        assertThat(outRecords.get(0).value().getDeleted$1()).isFalse();
        assertThat(outRecords.get(outRecords.size() - 1).value().getDeleted$1()).isTrue();
    }



}
