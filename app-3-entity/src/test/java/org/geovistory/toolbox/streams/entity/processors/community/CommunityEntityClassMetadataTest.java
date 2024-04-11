package org.geovistory.toolbox.streams.entity.processors.community;


import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.kafka.streams.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.InputTopicNames;
import org.geovistory.toolbox.streams.entity.OutputTopicNames;
import org.geovistory.toolbox.streams.testlib.TopologyTestDriverProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
class CommunityEntityClassMetadataTest {

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
    private TopologyTestDriver testDriver;
    private TestInputTopic<CommunityEntityKey, CommunityEntityValue> communityEntityTopic;
    private TestInputTopic<OntomeClassKey, OntomeClassMetadataValue> ontomeClassMetadataTopic;
    private TestOutputTopic<CommunityEntityKey, CommunityEntityClassMetadataValue> outputTopic;

    @BeforeEach
    void setup() {

        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");

        testDriver = new TopologyTestDriver(topology, props);

        testDriver = new TopologyTestDriver(topology, props);

        communityEntityTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityEntity(),
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.CommunityEntityValue().serializer());

        ontomeClassMetadataTopic = testDriver.createInputTopic(
                inputTopicNames.getOntomeClassMetadata(),
                avroSerdes.OntomeClassKey().serializer(),
                avroSerdes.OntomeClassMetadataValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.communityEntityClassMetadata(),
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
