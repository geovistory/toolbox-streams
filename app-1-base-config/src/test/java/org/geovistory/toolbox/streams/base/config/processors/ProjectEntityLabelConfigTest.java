package org.geovistory.toolbox.streams.base.config.processors;


import ts.projects.entity_label_config.Value;
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

class ProjectEntityLabelConfigTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectEntityLabelConfigTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ts.projects.entity_label_config.Key, Value> proEntityLabelConfigTopic;
    private TestInputTopic<ProjectClassKey, ProjectClassValue> projectClassTopic;
    private TestInputTopic<CommunityEntityLabelConfigKey, CommunityEntityLabelConfigValue> communityEntityLabelTopic;
    private TestOutputTopic<ProjectClassKey, ProjectEntityLabelConfigValue> outputTopic;

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
        var projectEntityLabelConfig = new ProjectEntityLabelConfig(avroSerdes, registerInputTopic, registerInnerTopic,outputTopicNames);
        projectEntityLabelConfig.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        proEntityLabelConfigTopic = testDriver.createInputTopic(
                inputTopicNames. proEntityLabelConfig(),
                avroSerdes.ProEntityLabelConfigKey().serializer(),
                avroSerdes.ProEntityLabelConfigValue().serializer());

        communityEntityLabelTopic = testDriver.createInputTopic(
                outputTopicNames. communityEntityLabelConfig(),
                avroSerdes.CommunityEntityLabelConfigKey().serializer(),
                avroSerdes.CommunityEntityLabelConfigValue().serializer());

        projectClassTopic = testDriver.createInputTopic(
                outputTopicNames. projectClass(),
                avroSerdes.ProjectClassKey().serializer(),
                avroSerdes.ProjectClassValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectEntityLabelConfig(),
                avroSerdes.ProjectClassKey().deserializer(),
                avroSerdes.ProjectEntityLabelConfigValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testClassWithBothConfigs() {
        var classId = 12;
        var projectId = 30;
        var expectedProperty = 33;
        // add class 12 to project 30
        var kC = ProjectClassKey.newBuilder()
                .setClassId(classId)
                .setProjectId(projectId)
                .build();
        var vC = ProjectClassValue.newBuilder()
                .setClassId(classId)
                .setProjectId(projectId)
                .build();
        projectClassTopic.pipeInput(kC, vC);


        // add entity label configs of project 12
        var kR = ts.projects.entity_label_config.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var config = EntityLabelConfig.newBuilder()
                .setLabelParts(List.of(
                        EntityLabelConfigPart.newBuilder()
                                .setOrdNum(2)
                                .setField(
                                        EntityLabelConfigPartField.newBuilder()
                                                .setFkProperty(expectedProperty)
                                                .setIsOutgoing(true)
                                                .setNrOfStatementsInLabel(2)
                                                .build()
                                )
                                .build()
                ))
                .build();

        var vR = Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(1)
                .setFkClass(classId)
                .setFkProject(projectId)
                .setConfig(config.toString())
                .build();

        proEntityLabelConfigTopic.pipeInput(kR, vR);

        // set community config
        var kCom = CommunityEntityLabelConfigKey.newBuilder()
                .setClassId(classId)
                .build();
        var config2 = EntityLabelConfig.newBuilder()
                .setLabelParts(List.of(
                        EntityLabelConfigPart.newBuilder()
                                .setOrdNum(44)
                                .setField(
                                        EntityLabelConfigPartField.newBuilder()
                                                .setFkProperty(44)
                                                .setIsOutgoing(true)
                                                .setNrOfStatementsInLabel(2)
                                                .build()
                                )
                                .build()
                )).build();
        var vCom = CommunityEntityLabelConfigValue.newBuilder()
                .setClassId(classId)
                .setConfig(config2)
                .build();
        communityEntityLabelTopic.pipeInput(kCom, vCom);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectClassKey.newBuilder()
                .setClassId(classId)
                .setProjectId(projectId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getConfig().getLabelParts().get(0).getField().getFkProperty()).isEqualTo(expectedProperty);
        assertThat(record.getDeleted$1()).isEqualTo(false);

    }

    @Test
    void testClassWithCommunityConfigOnly() {
        var classId = 12;
        var projectId = 30;
        var expectedProperty = 44;
        // add class 12 to project 30
        var kC = ProjectClassKey.newBuilder()
                .setClassId(classId)
                .setProjectId(projectId)
                .build();
        var vC = ProjectClassValue.newBuilder()
                .setClassId(classId)
                .setProjectId(projectId)
                .build();
        projectClassTopic.pipeInput(kC, vC);

        // set community config
        var kCom = CommunityEntityLabelConfigKey.newBuilder()
                .setClassId(classId)
                .build();
        var config2 = EntityLabelConfig.newBuilder()
                .setLabelParts(List.of(
                        EntityLabelConfigPart.newBuilder()
                                .setOrdNum(44)
                                .setField(
                                        EntityLabelConfigPartField.newBuilder()
                                                .setFkProperty(expectedProperty)
                                                .setIsOutgoing(true)
                                                .setNrOfStatementsInLabel(2)
                                                .build()
                                )
                                .build()
                )).build();
        var vCom = CommunityEntityLabelConfigValue.newBuilder()
                .setClassId(classId)
                .setConfig(config2)
                .build();
        communityEntityLabelTopic.pipeInput(kCom, vCom);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectClassKey.newBuilder()
                .setClassId(classId)
                .setProjectId(projectId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getConfig().getLabelParts().get(0).getField().getFkProperty()).isEqualTo(expectedProperty);
        assertThat(record.getDeleted$1()).isEqualTo(false);
    }

    @Test
    void testDeleteCommunityConfig() {
        var classId = 12;
        var projectId = 30;
        // add class 12 to project 30
        var kC = ProjectClassKey.newBuilder()
                .setClassId(classId)
                .setProjectId(projectId)
                .build();
        var vC = ProjectClassValue.newBuilder()
                .setClassId(classId)
                .setProjectId(projectId)
                .build();
        projectClassTopic.pipeInput(kC, vC);

        // set community config
        var kCom = CommunityEntityLabelConfigKey.newBuilder()
                .setClassId(classId)
                .build();
        var config2 = EntityLabelConfig.newBuilder()
                .setLabelParts(List.of(
                        EntityLabelConfigPart.newBuilder()
                                .setOrdNum(44)
                                .setField(
                                        EntityLabelConfigPartField.newBuilder()
                                                .setFkProperty(44)
                                                .setIsOutgoing(true)
                                                .setNrOfStatementsInLabel(2)
                                                .build()
                                )
                                .build()
                )).build();
        var vCom = CommunityEntityLabelConfigValue.newBuilder()
                .setClassId(classId)
                .setConfig(config2)
                .build();
        communityEntityLabelTopic.pipeInput(kCom, vCom);
        // delete
        vCom.setDeleted$1(true);
        communityEntityLabelTopic.pipeInput(kCom, vCom);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectClassKey.newBuilder()
                .setClassId(classId)
                .setProjectId(projectId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getDeleted$1()).isEqualTo(true);
    }

    @Test
    void testClassWithProjectConfigOnly() {
        var classId = 12;
        var projectId = 30;
        var expectedProperty = 33;
        // add class 12 to project 30
        var kC = ProjectClassKey.newBuilder()
                .setClassId(classId)
                .setProjectId(projectId)
                .build();
        var vC = ProjectClassValue.newBuilder()
                .setClassId(classId)
                .setProjectId(projectId)
                .build();
        projectClassTopic.pipeInput(kC, vC);


        // add entity label configs of project 12
        var kR = ts.projects.entity_label_config.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var config = EntityLabelConfig.newBuilder()
                .setLabelParts(List.of(
                        EntityLabelConfigPart.newBuilder()
                                .setOrdNum(2)
                                .setField(
                                        EntityLabelConfigPartField.newBuilder()
                                                .setFkProperty(expectedProperty)
                                                .setIsOutgoing(true)
                                                .setNrOfStatementsInLabel(2)
                                                .build()
                                )
                                .build()
                ))
                .build();

        var vR = Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(1)
                .setFkClass(classId)
                .setFkProject(projectId)
                .setConfig(config.toString())
                .build();

        proEntityLabelConfigTopic.pipeInput(kR, vR);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectClassKey.newBuilder()
                .setClassId(classId)
                .setProjectId(projectId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getConfig().getLabelParts().get(0).getField().getFkProperty()).isEqualTo(expectedProperty);
        assertThat(record.getDeleted$1()).isEqualTo(false);

    }

    @Test
    void testDeleteProjectConfig() {
        var classId = 12;
        var projectId = 30;
        var expectedProperty = 33;
        // add class 12 to project 30
        var kC = ProjectClassKey.newBuilder()
                .setClassId(classId)
                .setProjectId(projectId)
                .build();
        var vC = ProjectClassValue.newBuilder()
                .setClassId(classId)
                .setProjectId(projectId)
                .build();
        projectClassTopic.pipeInput(kC, vC);


        // add entity label configs of project 12
        var kR = ts.projects.entity_label_config.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var config = EntityLabelConfig.newBuilder()
                .setLabelParts(List.of(
                        EntityLabelConfigPart.newBuilder()
                                .setOrdNum(2)
                                .setField(
                                        EntityLabelConfigPartField.newBuilder()
                                                .setFkProperty(expectedProperty)
                                                .setIsOutgoing(true)
                                                .setNrOfStatementsInLabel(2)
                                                .build()
                                )
                                .build()
                ))
                .build();

        var vR = Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(1)
                .setFkClass(classId)
                .setFkProject(projectId)
                .setConfig(config.toString())
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
        var resultingKey = ProjectClassKey.newBuilder()
                .setClassId(classId)
                .setProjectId(projectId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getDeleted$1()).isEqualTo(true);
    }

}
