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

import java.util.ArrayList;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectClassTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectClassTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<OntomeClassKey, OntomeClassValue> ontomeClassTopic;
    private TestInputTopic<ProjectProfileKey, ProjectProfileValue> projectProfilesTopic;
    private TestOutputTopic<ProjectClassKey, ProjectClassValue> outputTopic;

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
        var projectClass = new ProjectClass(avroSerdes, registerInputTopic, registerInnerTopic,outputTopicNames);
        projectClass.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);


        ontomeClassTopic = testDriver.createInputTopic(
                inputTopicNames. ontomeClass(),
                avroSerdes.OntomeClassKey().serializer(),
                avroSerdes.OntomeClassValue().serializer());

        projectProfilesTopic = testDriver.createInputTopic(
                outputTopicNames. projectProfile(),
                avroSerdes.ProjectProfileKey().serializer(),
                avroSerdes.ProjectProfileValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectClass(),
                avroSerdes.ProjectClassKey().deserializer(),
                avroSerdes.ProjectClassValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testOneProjectProfileAndOneApiClass() {
        // add project profile rel
        var ppKey = ProjectProfileKey.newBuilder()
                .setProjectId(1)
                .setProfileId(97)
                .build();
        var ppVal = ProjectProfileValue.newBuilder()
                .setProjectId(1)
                .setProfileId(97)
                .setDeleted$1(false)
                .build();
        projectProfilesTopic.pipeInput(ppKey, ppVal);

        // add class
        var apKey = new OntomeClassKey(44);
        var apVal = OntomeClassValue.newBuilder()
                .setDfhAncestorClasses(new ArrayList<>())
                .setDfhParentClasses(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPkClass(44)
                .build();
        ontomeClassTopic.pipeInput(apKey, apVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var projectClassKey = ProjectClassKey.newBuilder()
                .setProjectId(1)
                .setClassId(44)
                .build();
        assertThat(outRecords.get(projectClassKey).getDeleted$1()).isFalse();
    }


    @Test
    void testTwoProjectsProfilesAndTwoApiClass() {
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

        // add first class
        var apKey = new OntomeClassKey(44);
        var apVal = OntomeClassValue.newBuilder()
                .setDfhAncestorClasses(new ArrayList<>())
                .setDfhParentClasses(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPkClass(44)
                .build();
        ontomeClassTopic.pipeInput(apKey, apVal);

        // add second class
        apKey.setClassId(45);
        apVal.setDfhPkClass(45);
        ontomeClassTopic.pipeInput(apKey, apVal);
        // add third class
        apKey.setClassId(46);
        apVal.setDfhFkProfile(98);
        apVal.setDfhPkClass(46);
        ontomeClassTopic.pipeInput(apKey, apVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(3);
        var projectClassKey = ProjectClassKey.newBuilder().
                setProjectId(1)
                .setClassId(44)
                .build();
        assertThat(outRecords.get(projectClassKey).getDeleted$1()).isFalse();
        projectClassKey.setClassId(45);
        assertThat(outRecords.get(projectClassKey).getDeleted$1()).isFalse();
        projectClassKey.setClassId(46);
        assertThat(outRecords.get(projectClassKey).getDeleted$1()).isFalse();
    }

    @Test
    void testOneProfileInTwoProjects() {
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
        pKey.setProjectId(2);
        pVal.setProjectId(2);
        projectProfilesTopic.pipeInput(pKey, pVal);


        // add first class
        var apKey = new OntomeClassKey(44);
        var apVal = OntomeClassValue.newBuilder()
                .setDfhAncestorClasses(new ArrayList<>())
                .setDfhParentClasses(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPkClass(44)
                .build();
        ontomeClassTopic.pipeInput(apKey, apVal);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);

        // assert class in project 1
        var projectClassKey = ProjectClassKey.newBuilder()
                .setProjectId(1)
                .setClassId(44)
                .build();
        assertThat(outRecords.get(projectClassKey).getDeleted$1()).isFalse();

        // assert class in project 2
        projectClassKey.setProjectId(2);
        assertThat(outRecords.get(projectClassKey).getDeleted$1()).isFalse();
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

        // add class
        var apKey = new OntomeClassKey(44);
        var apVal = OntomeClassValue.newBuilder()
                .setDfhAncestorClasses(new ArrayList<>())
                .setDfhParentClasses(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPkClass(44)
                .build();
        ontomeClassTopic.pipeInput(apKey, apVal);

        // mark project profile as deleted
        pVal.setDeleted$1(true);
        projectProfilesTopic.pipeInput(pKey, pVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        var projectClassKey = ProjectClassKey.newBuilder().
                setProjectId(1)
                .setClassId(44)
                .build();
        assertThat(outRecords.get(projectClassKey).getDeleted$1()).isTrue();
    }


    @Test
    void testMarkOneClassAsDeleted() {
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

        // add class
        var apKey = new OntomeClassKey(44);
        var apVal = OntomeClassValue.newBuilder()
                .setDfhAncestorClasses(new ArrayList<>())
                .setDfhParentClasses(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPkClass(44)
                .build();
        ontomeClassTopic.pipeInput(apKey, apVal);

        // mark class as deleted
        apVal.setDeleted$1("true");
        ontomeClassTopic.pipeInput(apKey, apVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();

        assertThat(outRecords).hasSize(1);
        var projectClassKey = ProjectClassKey.newBuilder()
                .setProjectId(1)
                .setClassId(44)
                .build();
        assertThat(outRecords.get(projectClassKey).getDeleted$1()).isTrue();
    }

}
