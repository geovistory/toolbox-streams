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
class RequiredProfilesTopologyTest {

    // will be shared between test methods
    @Container
    private static final GenericContainer<?> APICURIO_CONTAINER = new GenericContainer<>(DockerImageName.parse("apicurio/apicurio-registry-mem:2.3.1.Final"))
            .withExposedPorts(8080);

    private TopologyTestDriver testDriver;
    private TestInputTopic<dev.system.config.Key, dev.system.config.Value> configTopic;
    private TestOutputTopic<Integer, Integer> outputTopic;

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

        Topology topology = RequiredProfilesTopology.build(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        configTopic = testDriver.createInputTopic(
                RequiredProfilesTopology.input.TOPICS.config,
                AvroSerdes.SysConfigKey().serializer(),
                AvroSerdes.SysConfigValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                RequiredProfilesTopology.output.TOPICS.required_profiles,
                Serdes.Integer().deserializer(),
                Serdes.Integer().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }


    @Test
    void testOneRequiredProfile() {


        var cKey = new dev.system.config.Key(1);
        var cVal = dev.system.config.Value.newBuilder().setSchemaName("").setTableName("")
                .setKey("SYS_CONFIG")
                .setConfig("{\"ontome\": {\"requiredOntomeProfiles\": [5]}}").build();
        configTopic.pipeInput(cKey, cVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        assertThat(outRecords.containsKey(5)).isTrue();
    }

    @Test
    void testTwoRequiredProfiles() {

        var cKey = new dev.system.config.Key(1);
        var cVal = dev.system.config.Value.newBuilder().setSchemaName("").setTableName("")
                .setKey("SYS_CONFIG")
                .setConfig("{\"ontome\": {\"requiredOntomeProfiles\": [5, 97]}}").build();
        configTopic.pipeInput(cKey, cVal);

        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);
        assertThat(outRecords.containsKey(5)).isTrue();
        assertThat(outRecords.containsKey(97)).isTrue();
    }

    @Test
    void testRemoveRequiredProfile() {

        var cKey = new dev.system.config.Key(1);
        var cVal = dev.system.config.Value.newBuilder().setSchemaName("").setTableName("")
                .setKey("SYS_CONFIG")
                .setConfig("{\"ontome\": {\"requiredOntomeProfiles\": [5]}}").build();
        configTopic.pipeInput(cKey, cVal);

        cVal.setConfig("{\"ontome\": {\"requiredOntomeProfiles\": []}}");
        configTopic.pipeInput(cKey, cVal);

        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        assertThat(outRecords.get(5)).isNull();
    }


}
