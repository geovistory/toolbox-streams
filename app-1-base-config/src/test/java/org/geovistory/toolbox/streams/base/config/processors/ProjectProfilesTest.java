package org.geovistory.toolbox.streams.base.config.processors;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.ProjectProfileKey;
import org.geovistory.toolbox.streams.avro.ProjectProfileValue;
import org.geovistory.toolbox.streams.base.config.*;
import org.geovistory.toolbox.streams.lib.ConfiguredAvroSerde;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectProfilesTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectProfilesTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

    private TopologyTestDriver testDriver;
    private TestInputTopic<dev.projects.dfh_profile_proj_rel.Key, dev.projects.dfh_profile_proj_rel.Value> profileProjectTopic;
    private TestInputTopic<dev.system.config.Key, dev.system.config.Value> configTopic;
    private TestInputTopic<dev.projects.project.Key, dev.projects.project.Value> projectTopic;
    private TestOutputTopic<ProjectProfileKey, ProjectProfileValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");

        var builderSingleton = new BuilderSingleton();
        var as = new ConfiguredAvroSerde();
        as.schemaRegistryUrl = MOCK_SCHEMA_REGISTRY_URL;
        var inputTopicNames = new InputTopicNames();
        var outputTopicNames = new OutputTopicNames();
        var registerInputTopic = new RegisterInputTopic(as, builderSingleton, inputTopicNames);
        var registerInnerTopic = new RegisterInnerTopic(as, builderSingleton, outputTopicNames);
        var projectProfiles = new ProjectProfiles(as, registerInputTopic, registerInnerTopic, outputTopicNames);
        projectProfiles.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        projectTopic = testDriver.createInputTopic(
                inputTopicNames.proProject(),
                as.<dev.projects.project.Key>key().serializer(),
                as.<dev.projects.project.Value>value().serializer());

        profileProjectTopic = testDriver.createInputTopic(
                inputTopicNames.proProfileProjRel(),
                as.<dev.projects.dfh_profile_proj_rel.Key>key().serializer(),
                as.<dev.projects.dfh_profile_proj_rel.Value>value().serializer());

        configTopic = testDriver.createInputTopic(
                inputTopicNames.sysConfig(),
                as.<dev.system.config.Key>key().serializer(),
                as.<dev.system.config.Value>value().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectProfile(),
                as.<ProjectProfileKey>key().deserializer(),
                as.<ProjectProfileValue>value().deserializer());
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
        assertThat(outputTopic.isEmpty()).isTrue();
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
        assertThat(outRecords.get(new ProjectProfileKey(20, 100)).getDeleted$1()).isFalse();
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
        assertThat(outRecords.get(new ProjectProfileKey(20, 5)).getDeleted$1()).isFalse();
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
        assertThat(outRecords).hasSize(4);
        assertThat(outRecords.get(new ProjectProfileKey(20, 100)).getDeleted$1()).isFalse();
        assertThat(outRecords.get(new ProjectProfileKey(20, 101)).getDeleted$1()).isFalse();
        assertThat(outRecords.get(new ProjectProfileKey(20, 5)).getDeleted$1()).isFalse();
        assertThat(outRecords.get(new ProjectProfileKey(20, 97)).getDeleted$1()).isFalse();
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
        assertThat(outRecords).hasSize(1);
        assertThat(outRecords.get(new ProjectProfileKey(20, 100)).getDeleted$1()).isTrue();
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
        assertThat(outRecords.get(new ProjectProfileKey(20, 5)).getDeleted$1()).isTrue();
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
        assertThat(outRecords.get(new ProjectProfileKey(20, 100)).getDeleted$1()).isTrue();
    }


    @Test
    void testDeleteSysConfig() {
        var pKey = new dev.projects.project.Key(20);
        var pVal = dev.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

        var cKey = new dev.system.config.Key(1);
        var cVal = dev.system.config.Value.newBuilder().setSchemaName("").setTableName("")
                .setKey("SYS_CONFIG")
                .setConfig("{\"ontome\": {\"requiredOntomeProfiles\": [5]}}").build();
        configTopic.pipeInput(cKey, cVal);

        cKey = new dev.system.config.Key(1);
        cVal = dev.system.config.Value.newBuilder().setSchemaName("").setTableName("")
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
        var pKey = new dev.projects.project.Key(20);
        var pVal = dev.projects.project.Value.newBuilder().build();
        projectTopic.pipeInput(pKey, pVal);

        var cKey = new dev.system.config.Key(1);
        var cVal = dev.system.config.Value.newBuilder().setSchemaName("").setTableName("")
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
