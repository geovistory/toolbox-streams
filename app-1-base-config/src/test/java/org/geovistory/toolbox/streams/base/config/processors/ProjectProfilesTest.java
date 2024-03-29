package org.geovistory.toolbox.streams.base.config.processors;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.ProjectProfileKey;
import org.geovistory.toolbox.streams.avro.ProjectProfileValue;
import org.geovistory.toolbox.streams.base.config.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectProfilesTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectProfilesTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

    private TopologyTestDriver testDriver;
    private TestInputTopic<ts.projects.dfh_profile_proj_rel.Key, ts.projects.dfh_profile_proj_rel.Value> profileProjectTopic;
    private TestInputTopic<ts.system.config.Key, ts.system.config.Value> configTopic;
    private TestInputTopic<ts.projects.project.Key, ts.projects.project.Value> projectTopic;
    private TestOutputTopic<ProjectProfileKey, ProjectProfileValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");

        var builderSingleton = new BuilderSingleton();
        var avroSerdes = new AvroSerdes();
        avroSerdes.QUARKUS_KAFKA_STREAMS_SCHEMA_REGISTRY_URL = MOCK_SCHEMA_REGISTRY_URL;
        var inputTopicNames = new InputTopicNames();
        var outputTopicNames = new OutputTopicNames();
        var registerInputTopic = new RegisterInputTopic(avroSerdes, builderSingleton, inputTopicNames);
        var registerInnerTopic = new RegisterInnerTopic(avroSerdes, builderSingleton, outputTopicNames);
        var projectProfiles = new ProjectProfiles(avroSerdes, registerInputTopic, registerInnerTopic, outputTopicNames);
        projectProfiles.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        projectTopic = testDriver.createInputTopic(
                inputTopicNames.proProject(),
                avroSerdes.ProProjectKey().serializer(),
                avroSerdes.ProProjectValue().serializer());

        profileProjectTopic = testDriver.createInputTopic(
                inputTopicNames.proProfileProjRel(),
                avroSerdes.ProProfileProjRelKey().serializer(),
                avroSerdes.ProProfileProjRelValue().serializer());

        configTopic = testDriver.createInputTopic(
                inputTopicNames.sysConfig(),
                avroSerdes.SysConfigKey().serializer(),
                avroSerdes.SysConfigValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectProfile(),
                avroSerdes.ProjectProfileKey().deserializer(),
                avroSerdes.ProjectProfileValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testZeroProfiles() {
        var pKey = new ts.projects.project.Key(20);
        var pVal = ts.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    void testOneEnabledProfile() {
        var pKey = new ts.projects.project.Key(20);
        var pVal = ts.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

        var ppKey1 = new ts.projects.dfh_profile_proj_rel.Key(4);
        var ppVal1 = ts.projects.dfh_profile_proj_rel.Value.newBuilder()
                .setFkProject(20).setFkProfile(100).setEnabled(true).build();
        profileProjectTopic.pipeInput(ppKey1, ppVal1);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        assertThat(outRecords.get(new ProjectProfileKey(20, 100)).getDeleted$1()).isFalse();
    }


    @Test
    void testOneRequiredProfile() {
        var pKey = new ts.projects.project.Key(20);
        var pVal = ts.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

        var cKey = new ts.system.config.Key(1);
        var cVal = ts.system.config.Value.newBuilder().setSchemaName("").setTableName("")
                .setKey("SYS_CONFIG")
                .setConfig("{\"ontome\": {\"requiredOntomeProfiles\": [5]}}").build();
        configTopic.pipeInput(cKey, cVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        assertThat(outRecords.get(new ProjectProfileKey(20, 5)).getDeleted$1()).isFalse();
    }

    @Test
    void testTwoEnabledAndTwoRequiredProfiles() {
        var pKey = new ts.projects.project.Key(20);
        var pVal = ts.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

        var ppKey = new ts.projects.dfh_profile_proj_rel.Key(4);
        var ppVal = ts.projects.dfh_profile_proj_rel.Value.newBuilder()
                .setFkProject(20).setFkProfile(100).setEnabled(true).build();
        profileProjectTopic.pipeInput(ppKey, ppVal);

        ppKey.setPkEntity(2);
        ppVal.setFkProfile(101);
        profileProjectTopic.pipeInput(ppKey, ppVal);

        var cKey = new ts.system.config.Key(1);
        var cVal = ts.system.config.Value.newBuilder().setSchemaName("").setTableName("")
                .setKey("SYS_CONFIG")
                .setConfig("{\"ontome\": {\"requiredOntomeProfiles\": [5, 97]}}").build();
        configTopic.pipeInput(cKey, cVal);

        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(4);
        assertThat(outRecords.get(new ProjectProfileKey(20, 100)).getDeleted$1()).isFalse();
        assertThat(outRecords.get(new ProjectProfileKey(20, 101)).getDeleted$1()).isFalse();
        assertThat(outRecords.get(new ProjectProfileKey(20, 5)).getDeleted$1()).isFalse();
        assertThat(outRecords.get(new ProjectProfileKey(20, 97)).getDeleted$1()).isFalse();
    }


    @Test
    void testRemoveEnabledProfile() {
        var pKey = new ts.projects.project.Key(20);
        var pVal = ts.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

        var ppKey = new ts.projects.dfh_profile_proj_rel.Key(1);
        var ppVal = ts.projects.dfh_profile_proj_rel.Value.newBuilder()
                .setFkProject(20).setFkProfile(100).setEnabled(true).build();
        profileProjectTopic.pipeInput(ppKey, ppVal);

        ppKey.setPkEntity(2);
        ppVal.setFkProfile(101);
        profileProjectTopic.pipeInput(ppKey, ppVal);

        // remove first record (tombstone)
        ppKey.setPkEntity(1);
        ppVal.setDeleted$1("true");
        profileProjectTopic.pipeInput(ppKey, ppVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);
        assertThat(outRecords.get(new ProjectProfileKey(20, 100)).getDeleted$1()).isTrue();
        assertThat(outRecords.get(new ProjectProfileKey(20, 101)).getDeleted$1()).isFalse();
    }


    @Test
    void testDisableEnabledProfile() {
        var pKey = new ts.projects.project.Key(20);
        var pVal = ts.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

        var ppKey = new ts.projects.dfh_profile_proj_rel.Key(1);
        var ppVal = ts.projects.dfh_profile_proj_rel.Value.newBuilder()
                .setFkProject(20).setFkProfile(100).setEnabled(true).build();
        profileProjectTopic.pipeInput(ppKey, ppVal);

        ppKey.setPkEntity(1);
        ppVal.setEnabled(false);
        profileProjectTopic.pipeInput(ppKey, ppVal);

        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        assertThat(outRecords.get(new ProjectProfileKey(20, 100)).getDeleted$1()).isTrue();
    }


    @Test
    void testRemoveRequiredProfile() {
        var pKey = new ts.projects.project.Key(20);
        var pVal = ts.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

        var cKey = new ts.system.config.Key(1);
        var cVal = ts.system.config.Value.newBuilder().setSchemaName("").setTableName("")
                .setKey("SYS_CONFIG")
                .setConfig("{\"ontome\": {\"requiredOntomeProfiles\": [5]}}").build();
        configTopic.pipeInput(cKey, cVal);

        cVal.setConfig("{\"ontome\": {\"requiredOntomeProfiles\": []}}");
        configTopic.pipeInput(cKey, cVal);

        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        assertThat(outRecords.get(new ProjectProfileKey(20, 5)).getDeleted$1()).isTrue();
    }


    @Test
    void testRemoveProjectTombstone() {

        var pKey = new ts.projects.project.Key(20);
        var pVal = ts.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

        var ppKey = new ts.projects.dfh_profile_proj_rel.Key(4);
        var ppVal = ts.projects.dfh_profile_proj_rel.Value.newBuilder()
                .setFkProject(20).setFkProfile(100).setEnabled(true).build();
        profileProjectTopic.pipeInput(ppKey, ppVal);

        projectTopic.pipeInput(pKey, null);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        assertThat(outRecords.get(new ProjectProfileKey(20, 100)).getDeleted$1()).isTrue();
    }


    @Test
    void testDeleteSysConfig() {
        var pKey = new ts.projects.project.Key(20);
        var pVal = ts.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

        var cKey = new ts.system.config.Key(1);
        var cVal = ts.system.config.Value.newBuilder().setSchemaName("").setTableName("")
                .setKey("SYS_CONFIG")
                .setConfig("{\"ontome\": {\"requiredOntomeProfiles\": [5]}}").build();
        configTopic.pipeInput(cKey, cVal);

        cKey = new ts.system.config.Key(1);
        cVal = ts.system.config.Value.newBuilder().setSchemaName("").setTableName("")
                .setKey(null)
                .setConfig(null)
                .setDeleted$1("true").build();
        configTopic.pipeInput(cKey, cVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        assertThat(outRecords.get(new ProjectProfileKey(20, 5)).getDeleted$1()).isFalse();
    }

    /**
     * Test that output topic has no new message, if
     * system config is changed but the requiredOntomeProfiles are untouched
     */
    @Test
    void testUpdateSysConfigSuppressUnchangedProfiles() {
        var pKey = new ts.projects.project.Key(20);
        var pVal = ts.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

        var cKey = new ts.system.config.Key(1);
        var cVal = ts.system.config.Value.newBuilder().setSchemaName("").setTableName("")
                .setKey("SYS_CONFIG")
                .setConfig("{\"ontome\": {\"requiredOntomeProfiles\": [5]}}").build();
        configTopic.pipeInput(cKey, cVal);

        // pipe it again!
        configTopic.pipeInput(cKey, cVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readRecordsToList();
        assertThat(outRecords).hasSize(1);
    }

}
