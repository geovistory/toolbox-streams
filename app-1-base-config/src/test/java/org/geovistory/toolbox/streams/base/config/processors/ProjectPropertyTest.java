package org.geovistory.toolbox.streams.base.config.processors;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectPropertyTest {


    private static final String SCHEMA_REGISTRY_SCOPE = ProjectPropertyTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<OntomePropertyKey, OntomePropertyValue> ontomePropertyTopic;
    private TestInputTopic<ProjectProfileKey, ProjectProfileValue> projectProfilesTopic;
    private TestOutputTopic<ProjectPropertyKey, ProjectPropertyValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);


        Topology topology = ProjectProperty.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        ontomePropertyTopic = testDriver.createInputTopic(
                ProjectProperty.input.TOPICS.ontome_property,
                avroSerdes.OntomePropertyKey().serializer(),
                avroSerdes.OntomePropertyValue().serializer());

        projectProfilesTopic = testDriver.createInputTopic(
                ProjectProperty.input.TOPICS.project_profile,
                avroSerdes.ProjectProfileKey().serializer(),
                avroSerdes.ProjectProfileValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                ProjectProperty.output.TOPICS.project_property,
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

        // add property
        var apKey = new OntomePropertyKey(44);
        var apVal = OntomePropertyValue.newBuilder()
                .setDfhAncestorProperties(new ArrayList<>())
                .setDfhParentProperties(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPropertyDomain(33)
                .setDfhPkProperty(44)
                .setDfhPropertyRange(55)
                .build();
        ontomePropertyTopic.pipeInput(apKey, apVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var projectPropertyKey = ProjectPropertyKey.newBuilder()
                .setProjectId(1)
                .setDomainId(33)
                .setPropertyId(44)
                .setRangeId(55)
                .build();
        assertThat(outRecords.get(projectPropertyKey).getDeleted$1()).isFalse();
    }

    @Test
    void testTwoProjectsProfilesAndTwoApiPropertySize() {
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

        // add first property in profile 97
        var apKey = new OntomePropertyKey(44);
        var apVal = OntomePropertyValue.newBuilder()
                .setDfhAncestorProperties(new ArrayList<>())
                .setDfhParentProperties(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPropertyDomain(33)
                .setDfhPkProperty(44)
                .setDfhPropertyRange(55)
                .build();
        ontomePropertyTopic.pipeInput(apKey, apVal);

        // add first property in profile 98
        apVal.setDfhFkProfile(98);
        ontomePropertyTopic.pipeInput(apKey, apVal);

        // add second property
        apKey.setPropertyId(45);
        apVal.setDfhPkProperty(45);
        ontomePropertyTopic.pipeInput(apKey, apVal);
        // add third property
        apKey.setPropertyId(46);
        apVal.setDfhFkProfile(98);
        apVal.setDfhPkProperty(46);
        ontomePropertyTopic.pipeInput(apKey, apVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readRecordsToList();
        assertThat(outRecords).hasSize(3);
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
        var apKey = new OntomePropertyKey(44);
        var apVal = OntomePropertyValue.newBuilder()
                .setDfhAncestorProperties(new ArrayList<>())
                .setDfhParentProperties(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPropertyDomain(33)
                .setDfhPkProperty(44)
                .setDfhPropertyRange(55)
                .build();
        ontomePropertyTopic.pipeInput(apKey, apVal);

        // add second property
        apKey.setPropertyId(45);
        apVal.setDfhPkProperty(45);
        ontomePropertyTopic.pipeInput(apKey, apVal);
        // add third property
        apKey.setPropertyId(46);
        apVal.setDfhFkProfile(98);
        apVal.setDfhPkProperty(46);
        ontomePropertyTopic.pipeInput(apKey, apVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(3);
        var projectPropertyKey = ProjectPropertyKey.newBuilder().
                setProjectId(1)
                .setDomainId(33)
                .setPropertyId(44)
                .setRangeId(55)
                .build();
        assertThat(outRecords.get(projectPropertyKey).getDeleted$1()).isFalse();
        projectPropertyKey.setPropertyId(45);
        assertThat(outRecords.get(projectPropertyKey).getDeleted$1()).isFalse();
        projectPropertyKey.setPropertyId(46);
        assertThat(outRecords.get(projectPropertyKey).getDeleted$1()).isFalse();
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


        // add first property
        var apKey = new OntomePropertyKey(44);
        var apVal = OntomePropertyValue.newBuilder()
                .setDfhAncestorProperties(new ArrayList<>())
                .setDfhParentProperties(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPropertyDomain(33)
                .setDfhPkProperty(44)
                .setDfhPropertyRange(55)
                .build();
        ontomePropertyTopic.pipeInput(apKey, apVal);


      /*  // add second property
        apKey.setPkEntity(2);
        apVal.setDfhPkProperty(45);
        apiPropertyTopic.pipeInput(apKey, apVal);

        // add third property
        apVal.setDfhPkProperty(46);
        apiPropertyTopic.pipeInput(apKey, apVal);*/

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);

        // assert property in project 1
        var projectPropertyKey = ProjectPropertyKey.newBuilder()
                .setProjectId(1)
                .setDomainId(33)
                .setPropertyId(44)
                .setRangeId(55)
                .build();
        assertThat(outRecords.get(projectPropertyKey).getDeleted$1()).isFalse();

        // assert property in project 2
        projectPropertyKey.setProjectId(2);
        assertThat(outRecords.get(projectPropertyKey).getDeleted$1()).isFalse();
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
        var apKey = new OntomePropertyKey(44);
        var apVal = OntomePropertyValue.newBuilder()
                .setDfhAncestorProperties(new ArrayList<>())
                .setDfhParentProperties(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPropertyDomain(33)
                .setDfhPkProperty(44)
                .setDfhPropertyRange(55)
                .build();
        ontomePropertyTopic.pipeInput(apKey, apVal);

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
        var apKey = new OntomePropertyKey(44);
        var apVal = OntomePropertyValue.newBuilder()
                .setDfhAncestorProperties(new ArrayList<>())
                .setDfhParentProperties(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPropertyDomain(33)
                .setDfhPkProperty(44)
                .setDfhPropertyRange(55)
                .build();
        ontomePropertyTopic.pipeInput(apKey, apVal);

        // mark property as deleted
        apVal.setDeleted$1("true");
        ontomePropertyTopic.pipeInput(apKey, apVal);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();

        assertThat(outRecords).hasSize(1);
        var projectPropertyKey = ProjectPropertyKey.newBuilder()
                .setProjectId(1)
                .setDomainId(33)
                .setPropertyId(44)
                .setRangeId(55)
                .build();
        assertThat(outRecords.get(projectPropertyKey).getDeleted$1()).isTrue();
    }

  /*  @Test
    void testRedundantProperties() {
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
        var apKey = new OntomePropertyKey(1);
        var apVal = OntomePropertyValue.newBuilder()
                .setPkEntity(1)
                .setDfhAncestorProperties(new ArrayList<>())
                .setDfhParentProperties(new ArrayList<>())
                .setDfhFkProfile(97)
                .setDfhPropertyDomain(33)
                .setDfhPkProperty(44)
                .setDfhPropertyRange(55)
                .build();
        apiPropertyTopic.pipeInput(apKey, apVal);

        // add redundant property
        apKey.setPkEntity(2);
        apVal.setPkEntity(2);
        apiPropertyTopic.pipeInput(apKey, apVal);

        assertThat(innerTopicProfileWithProjectProperties.isEmpty()).isFalse();
        var outRecords = innerTopicProfileWithProjectProperties.readKeyValuesToMap();

        var profileId = (Integer) 97;
        assertThat(outRecords.get(profileId).getMap().size()).isEqualTo(1);
    }
*/
}
