package org.geovistory.toolbox.streams.base.config.processors;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.base.config.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CommuntiyEntityLabelConfigTest {

    private static final String SCHEMA_REGISTRY_SCOPE = CommuntiyEntityLabelConfigTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ts.projects.entity_label_config.Key, ts.projects.entity_label_config.Value> proEntityLabelConfigTopic;
    private TestOutputTopic<CommunityEntityLabelConfigKey, CommunityEntityLabelConfigValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");


        var builderSingleton = new BuilderSingleton();
        var avroSerdes = new AvroSerdes();
        avroSerdes.QUARKUS_KAFKA_STREAMS_SCHEMA_REGISTRY_URL = MOCK_SCHEMA_REGISTRY_URL;
        var inputTopicNames = new InputTopicNames();
        var outputTopicNames = new OutputTopicNames();
        var registerInputTopic = new RegisterInputTopic(avroSerdes, builderSingleton, inputTopicNames);
        var registerInnerTopic = new RegisterInnerTopic(avroSerdes, builderSingleton, outputTopicNames);
        var communityEntityLabelConfig = new CommunityEntityLabelConfig(avroSerdes, registerInputTopic, registerInnerTopic, outputTopicNames);
        communityEntityLabelConfig.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);


        proEntityLabelConfigTopic = testDriver.createInputTopic(
                inputTopicNames.proEntityLabelConfig(),
                avroSerdes.ProEntityLabelConfigKey().serializer(),
                avroSerdes.ProEntityLabelConfigValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.communityEntityLabelConfig(),
                avroSerdes.CommunityEntityLabelConfigKey().deserializer(),
                avroSerdes.CommunityEntityLabelConfigValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testRecords() {
        var classId = 12;
        // add entity label configs of default project
        var kR = ts.projects.entity_label_config.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var config = EntityLabelConfig.newBuilder()
                .setLabelParts(List.of(
                        EntityLabelConfigPart.newBuilder()
                                .setOrdNum(2)
                                .setField(
                                        EntityLabelConfigPartField.newBuilder()
                                                .setFkProperty(1)
                                                .setIsOutgoing(true)
                                                .setNrOfStatementsInLabel(2)
                                                .build()
                                )
                                .build()
                ))
                .build()
                .toString();
        var vR = ts.projects.entity_label_config.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(1)
                .setFkClass(12)
                .setFkProject(I.DEFAULT_PROJECT.get())
                .setConfig(config)
                .build();

        proEntityLabelConfigTopic.pipeInput(kR, vR);

        // set config for not default project
        vR.setFkProject(1);
        proEntityLabelConfigTopic.pipeInput(kR, vR);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = CommunityEntityLabelConfigKey.newBuilder()
                .setClassId(classId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getConfig().getLabelParts().get(0).getField().getNrOfStatementsInLabel()).isEqualTo(2);
    }

    @Test
    void testDeleteRecord() {
        var classId = 12;
        // add entity label configs of default project
        var kR = ts.projects.entity_label_config.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var config = EntityLabelConfig.newBuilder()
                .setLabelParts(List.of(
                        EntityLabelConfigPart.newBuilder()
                                .setOrdNum(2)
                                .setField(
                                        EntityLabelConfigPartField.newBuilder()
                                                .setFkProperty(1)
                                                .setIsOutgoing(true)
                                                .setNrOfStatementsInLabel(2)
                                                .build()
                                )
                                .build()
                ))
                .build()
                .toString();
        var vR = ts.projects.entity_label_config.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(1)
                .setFkClass(12)
                .setFkProject(I.DEFAULT_PROJECT.get())
                .setConfig(config)
                .build();

        proEntityLabelConfigTopic.pipeInput(kR, vR);

        // mimic Debezium delete.handling.mode=rewrite
        var vRewrite = ts.projects.entity_label_config.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(null)
                .setFkClass(null)
                .setFkProject(null)
                .setConfig(null)
                .setDeleted$1("true")
                .build();

        proEntityLabelConfigTopic.pipeInput(kR, vRewrite);


        // mimic Debezium drop.tombstones=false

        proEntityLabelConfigTopic.pipeInput(kR, null);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = CommunityEntityLabelConfigKey.newBuilder()
                .setClassId(classId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getDeleted$1()).isEqualTo(true);
    }
}
