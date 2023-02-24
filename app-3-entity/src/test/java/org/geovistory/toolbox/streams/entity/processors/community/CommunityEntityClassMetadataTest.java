package org.geovistory.toolbox.streams.entity.processors.community;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.Env;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CommunityEntityClassMetadataTest {

    private static final String SCHEMA_REGISTRY_SCOPE = CommunityEntityClassMetadataTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<CommunityEntityKey, CommunityEntityValue> communityEntityTopic;
    private TestInputTopic<OntomeClassKey, OntomeClassMetadataValue> ontomeClassMetadataTopic;
    private TestOutputTopic<CommunityEntityKey, CommunityEntityClassMetadataValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        var nameSupplement = "toolbox";
        Topology topology = CommunityEntityClassMetadata.buildStandalone(new StreamsBuilder(), nameSupplement);

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();


        communityEntityTopic = testDriver.createInputTopic(
                Env.INSTANCE.TOPIC_COMMUNITY_ENTITY,
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.CommunityEntityValue().serializer());

        ontomeClassMetadataTopic = testDriver.createInputTopic(
                Env.INSTANCE.TOPIC_ONTOME_CLASS_METADATA,
                avroSerdes.OntomeClassKey().serializer(),
                avroSerdes.OntomeClassMetadataValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                CommunityEntityClassMetadata.getOutputTopicName(nameSupplement),
                avroSerdes.CommunityEntityKey().deserializer(),
                avroSerdes.CommunityEntityClassMetadataValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }


    @Test
    void testCommunityEntityMetadata() {

        var entityId = "i1";
        var classId = 3;
        var parentClasses = List.of(1, 2, 3);
        var ancestorClasses = List.of(4, 5, 6);


        // add an entity
        var kE = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        var vE = CommunityEntityValue.newBuilder().setEntityId(entityId).setProjectCount(1).setClassId(3).build();
        communityEntityTopic.pipeInput(kE, vE);

        // add ontome class metadata
        var kS = OntomeClassKey.newBuilder().setClassId(classId).build();
        var vS = OntomeClassMetadataValue.newBuilder()
                .setAncestorClasses(ancestorClasses)
                .setParentClasses(parentClasses)
                .build();
        ontomeClassMetadataTopic.pipeInput(kS, vS);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = CommunityEntityKey.newBuilder()
                .setEntityId(entityId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getParentClasses()).contains(1, 2, 3);
        assertThat(record.getAncestorClasses()).contains(4, 5, 6);
        assertThat(record.getProjectCount()).isEqualTo(1);

    }


}
