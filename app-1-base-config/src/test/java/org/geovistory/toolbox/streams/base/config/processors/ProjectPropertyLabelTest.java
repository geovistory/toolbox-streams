package org.geovistory.toolbox.streams.base.config.processors;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.base.config.*;
import org.geovistory.toolbox.streams.lib.ConfiguredAvroSerde;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectPropertyLabelTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectPropertyLabelTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;

    private TestInputTopic<OntomePropertyLabelKey, OntomePropertyLabelValue> ontomePropertyLabelTopic;
    private TestInputTopic<GeovPropertyLabelKey, GeovPropertyLabelValue> geovPropertyLabelTopic;
    private TestInputTopic<ProjectPropertyKey, ProjectPropertyValue> projectPropertyTopic;
    private TestInputTopic<dev.projects.project.Key, dev.projects.project.Value> projectTopic;
    private TestOutputTopic<ProjectFieldLabelKey, ProjectFieldLabelValue> outputTopic;


    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        var builderSingleton = new BuilderSingleton();
        var as = new ConfiguredAvroSerde();
        as.schemaRegistryUrl = MOCK_SCHEMA_REGISTRY_URL;
        var inputTopicNames = new InputTopicNames();
        var outputTopicNames = new OutputTopicNames();
        var registerInputTopic = new RegisterInputTopic(as, builderSingleton, inputTopicNames);
        var registerInnerTopic = new RegisterInnerTopic(as, builderSingleton, outputTopicNames);
        var projectPropertyLabel = new ProjectPropertyLabel(as, registerInputTopic, registerInnerTopic, outputTopicNames);
        projectPropertyLabel.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        ontomePropertyLabelTopic = testDriver.createInputTopic(
                inputTopicNames.ontomePropertyLabel(),
                as.<OntomePropertyLabelKey>key().serializer(),
                as.<OntomePropertyLabelValue>value().serializer());

        geovPropertyLabelTopic = testDriver.createInputTopic(
                outputTopicNames.geovPropertyLabel(),
                as.<GeovPropertyLabelKey>key().serializer(),
                as.<GeovPropertyLabelValue>value().serializer());

        projectPropertyTopic = testDriver.createInputTopic(
                outputTopicNames.projectProperty(),
                as.<ProjectPropertyKey>key().serializer(),
                as.<ProjectPropertyValue>value().serializer());

        projectTopic = testDriver.createInputTopic(
                inputTopicNames.proProject(),
                as.<dev.projects.project.Key>key().serializer(),
                as.<dev.projects.project.Value>value().serializer());


        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectPropertyLabel(),
                as.<ProjectFieldLabelKey>key().deserializer(),
                as.<ProjectFieldLabelValue>value().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testOutgoingLabelInProjectLangFromGeovProject() {
        int domainId = 9;
        int propertyId = 10;
        int rangeId = 11;
        int projectId = 20;
        // add project
        var kP = dev.projects.project.Key.newBuilder()
                .setPkEntity(projectId)
                .build();
        var vP = dev.projects.project.Value.newBuilder()
                .setFkLanguage(I.DE.get())
                .build();
        projectTopic.pipeInput(kP, vP);

        // add project property
        var kC = ProjectPropertyKey.newBuilder()
                .setProjectId(projectId)
                .setDomainId(domainId)
                .setPropertyId(propertyId)
                .setRangeId(rangeId)
                .build();
        var vC = ProjectPropertyValue.newBuilder()
                .setProjectId(projectId)
                .setDomainId(domainId)
                .setPropertyId(propertyId)
                .setRangeId(rangeId)
                .build();
        projectPropertyTopic.pipeInput(kC, vC);

        // add ontome property label in english
        var kO = OntomePropertyLabelKey.newBuilder()
                .setPropertyId(propertyId)
                .setLanguageId(I.EN.get())
                .build();
        var vO = OntomePropertyLabelValue.newBuilder()
                .setPropertyId(propertyId)
                .setLanguageId(I.EN.get())
                .setLabel("label out 10 (from ontome en)")
                .setInverseLabel("label in 10 (from ontome en)")
                .build();
        ontomePropertyLabelTopic.pipeInput(kO, vO);

        // add geov default property out label in english
        var kG = GeovPropertyLabelKey.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.EN.get())
                .build();
        var vG = GeovPropertyLabelValue.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.EN.get())
                .setLabel("label out 10 (from geov default en)")
                .build();
        geovPropertyLabelTopic.pipeInput(kG, vG);

        // add geov project property out label in english
        kG = GeovPropertyLabelKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.EN.get())
                .build();
        vG = GeovPropertyLabelValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.EN.get())
                .setLabel("label out 10 (from geov project en)")
                .build();
        geovPropertyLabelTopic.pipeInput(kG, vG);

        // add ontome property out label in project lang
        kO = OntomePropertyLabelKey.newBuilder()
                .setPropertyId(propertyId)
                .setLanguageId(I.DE.get())
                .build();
        vO = OntomePropertyLabelValue.newBuilder()
                .setPropertyId(propertyId)
                .setLanguageId(I.DE.get())
                .setLabel("label out 10 (from ontome de)")
                .setInverseLabel("label in 10 (from ontome de)")
                .build();
        ontomePropertyLabelTopic.pipeInput(kO, vO);

        // add geov default property out label in project lang
        kG = GeovPropertyLabelKey.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.DE.get())
                .build();
        vG = GeovPropertyLabelValue.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.DE.get())
                .setLabel("label out 10 (from geov default de)")
                .build();
        geovPropertyLabelTopic.pipeInput(kG, vG);

        // add geov project property label in project lang
        kG = GeovPropertyLabelKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.DE.get())
                .build();
        vG = GeovPropertyLabelValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.DE.get())
                .setLabel("label out 10 (from geov project de)")
                .build();
        geovPropertyLabelTopic.pipeInput(kG, vG);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);
        var kOut = ProjectFieldLabelKey.newBuilder()
                .setClassId(domainId)
                .setIsOutgoing(true)
                .setProjectId(projectId)
                .setPropertyId(propertyId)
                .build();
        assertThat(outRecords.get(kOut).getLabel()).isEqualTo("label out 10 (from geov project de)");
        var kIn = ProjectFieldLabelKey.newBuilder()
                .setClassId(rangeId)
                .setIsOutgoing(false)
                .setProjectId(projectId)
                .setPropertyId(propertyId)
                .build();
        assertThat(outRecords.get(kIn).getLabel()).isEqualTo("label in 10 (from ontome de)");
    }

    @Test
    void testOutgoingLabelInProjectLangFromGeovDefaultProject() {
        int domainId = 9;
        int propertyId = 10;
        int rangeId = 11;
        int projectId = 20;
        // add project
        var kP = dev.projects.project.Key.newBuilder()
                .setPkEntity(projectId)
                .build();
        var vP = dev.projects.project.Value.newBuilder()
                .setFkLanguage(I.DE.get())
                .build();
        projectTopic.pipeInput(kP, vP);

        // add project property
        var kC = ProjectPropertyKey.newBuilder()
                .setProjectId(projectId)
                .setDomainId(domainId)
                .setPropertyId(propertyId)
                .setRangeId(rangeId)
                .build();
        var vC = ProjectPropertyValue.newBuilder()
                .setProjectId(projectId)
                .setDomainId(domainId)
                .setPropertyId(propertyId)
                .setRangeId(rangeId)
                .build();
        projectPropertyTopic.pipeInput(kC, vC);

        // add ontome property label in english
        var kO = OntomePropertyLabelKey.newBuilder()
                .setPropertyId(propertyId)
                .setLanguageId(I.EN.get())
                .build();
        var vO = OntomePropertyLabelValue.newBuilder()
                .setPropertyId(propertyId)
                .setLanguageId(I.EN.get())
                .setLabel("label out 10 (from ontome en)")
                .setInverseLabel("label in 10 (from ontome en)")
                .build();
        ontomePropertyLabelTopic.pipeInput(kO, vO);

        // add geov default property out label in english
        var kG = GeovPropertyLabelKey.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.EN.get())
                .build();
        var vG = GeovPropertyLabelValue.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.EN.get())
                .setLabel("label out 10 (from geov default en)")
                .build();
        geovPropertyLabelTopic.pipeInput(kG, vG);

        // add geov project property out label in english
        kG = GeovPropertyLabelKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.EN.get())
                .build();
        vG = GeovPropertyLabelValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.EN.get())
                .setLabel("label out 10 (from geov project en)")
                .build();
        geovPropertyLabelTopic.pipeInput(kG, vG);

        // add ontome property out label in project lang
        kO = OntomePropertyLabelKey.newBuilder()
                .setPropertyId(propertyId)
                .setLanguageId(I.DE.get())
                .build();
        vO = OntomePropertyLabelValue.newBuilder()
                .setPropertyId(propertyId)
                .setLanguageId(I.DE.get())
                .setLabel("label out 10 (from ontome de)")
                .setInverseLabel("label in 10 (from ontome de)")
                .build();
        ontomePropertyLabelTopic.pipeInput(kO, vO);

        // add geov default property out label in project lang
        kG = GeovPropertyLabelKey.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.DE.get())
                .build();
        vG = GeovPropertyLabelValue.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.DE.get())
                .setLabel("label out 10 (from geov default de)")
                .build();
        geovPropertyLabelTopic.pipeInput(kG, vG);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);
        var kOut = ProjectFieldLabelKey.newBuilder()
                .setClassId(domainId)
                .setIsOutgoing(true)
                .setProjectId(projectId)
                .setPropertyId(propertyId)
                .build();
        assertThat(outRecords.get(kOut).getLabel()).isEqualTo("label out 10 (from geov default de)");
        var kIn = ProjectFieldLabelKey.newBuilder()
                .setClassId(rangeId)
                .setIsOutgoing(false)
                .setProjectId(projectId)
                .setPropertyId(propertyId)
                .build();
        assertThat(outRecords.get(kIn).getLabel()).isEqualTo("label in 10 (from ontome de)");
    }

    @Test
    void testLabelInProjectLangFromOntome() {
        int domainId = 9;
        int propertyId = 10;
        int rangeId = 11;
        int projectId = 20;
        // add project
        var kP = dev.projects.project.Key.newBuilder()
                .setPkEntity(projectId)
                .build();
        var vP = dev.projects.project.Value.newBuilder()
                .setFkLanguage(I.DE.get())
                .build();
        projectTopic.pipeInput(kP, vP);

        // add project property
        var kC = ProjectPropertyKey.newBuilder()
                .setProjectId(projectId)
                .setDomainId(domainId)
                .setPropertyId(propertyId)
                .setRangeId(rangeId)
                .build();
        var vC = ProjectPropertyValue.newBuilder()
                .setProjectId(projectId)
                .setDomainId(domainId)
                .setPropertyId(propertyId)
                .setRangeId(rangeId)
                .build();
        projectPropertyTopic.pipeInput(kC, vC);

        // add ontome property label in english
        var kO = OntomePropertyLabelKey.newBuilder()
                .setPropertyId(propertyId)
                .setLanguageId(I.EN.get())
                .build();
        var vO = OntomePropertyLabelValue.newBuilder()
                .setPropertyId(propertyId)
                .setLanguageId(I.EN.get())
                .setLabel("label out 10 (from ontome en)")
                .setInverseLabel("label in 10 (from ontome en)")
                .build();
        ontomePropertyLabelTopic.pipeInput(kO, vO);

        // add geov default property out label in english
        var kG = GeovPropertyLabelKey.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.EN.get())
                .build();
        var vG = GeovPropertyLabelValue.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.EN.get())
                .setLabel("label out 10 (from geov default en)")
                .build();
        geovPropertyLabelTopic.pipeInput(kG, vG);

        // add geov project property out label in english
        kG = GeovPropertyLabelKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.EN.get())
                .build();
        vG = GeovPropertyLabelValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.EN.get())
                .setLabel("label out 10 (from geov project en)")
                .build();
        geovPropertyLabelTopic.pipeInput(kG, vG);

        // add ontome property out label in project lang
        kO = OntomePropertyLabelKey.newBuilder()
                .setPropertyId(propertyId)
                .setLanguageId(I.DE.get())
                .build();
        vO = OntomePropertyLabelValue.newBuilder()
                .setPropertyId(propertyId)
                .setLanguageId(I.DE.get())
                .setLabel("label out 10 (from ontome de)")
                .setInverseLabel("label in 10 (from ontome de)")
                .build();
        ontomePropertyLabelTopic.pipeInput(kO, vO);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);
        var kOut = ProjectFieldLabelKey.newBuilder()
                .setClassId(domainId)
                .setIsOutgoing(true)
                .setProjectId(projectId)
                .setPropertyId(propertyId)
                .build();
        assertThat(outRecords.get(kOut).getLabel()).isEqualTo("label out 10 (from ontome de)");
        var kIn = ProjectFieldLabelKey.newBuilder()
                .setClassId(rangeId)
                .setIsOutgoing(false)
                .setProjectId(projectId)
                .setPropertyId(propertyId)
                .build();
        assertThat(outRecords.get(kIn).getLabel()).isEqualTo("label in 10 (from ontome de)");
    }

    @Test
    void testLabelInEnglishFromGeovProject() {
        int domainId = 9;
        int propertyId = 10;
        int rangeId = 11;
        int projectId = 20;
        // add project
        var kP = dev.projects.project.Key.newBuilder()
                .setPkEntity(projectId)
                .build();
        var vP = dev.projects.project.Value.newBuilder()
                .setFkLanguage(I.DE.get())
                .build();
        projectTopic.pipeInput(kP, vP);

        // add project property
        var kC = ProjectPropertyKey.newBuilder()
                .setProjectId(projectId)
                .setDomainId(domainId)
                .setPropertyId(propertyId)
                .setRangeId(rangeId)
                .build();
        var vC = ProjectPropertyValue.newBuilder()
                .setProjectId(projectId)
                .setDomainId(domainId)
                .setPropertyId(propertyId)
                .setRangeId(rangeId)
                .build();
        projectPropertyTopic.pipeInput(kC, vC);

        // add ontome property label in english
        var kO = OntomePropertyLabelKey.newBuilder()
                .setPropertyId(propertyId)
                .setLanguageId(I.EN.get())
                .build();
        var vO = OntomePropertyLabelValue.newBuilder()
                .setPropertyId(propertyId)
                .setLanguageId(I.EN.get())
                .setLabel("label out 10 (from ontome en)")
                .setInverseLabel("label in 10 (from ontome en)")
                .build();
        ontomePropertyLabelTopic.pipeInput(kO, vO);

        // add geov default property out label in english
        var kG = GeovPropertyLabelKey.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.EN.get())
                .build();
        var vG = GeovPropertyLabelValue.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.EN.get())
                .setLabel("label out 10 (from geov default en)")
                .build();
        geovPropertyLabelTopic.pipeInput(kG, vG);

        // add geov project property out label in english
        kG = GeovPropertyLabelKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.EN.get())
                .build();
        vG = GeovPropertyLabelValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.EN.get())
                .setLabel("label out 10 (from geov project en)")
                .build();
        geovPropertyLabelTopic.pipeInput(kG, vG);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);
        var kOut = ProjectFieldLabelKey.newBuilder()
                .setClassId(domainId)
                .setIsOutgoing(true)
                .setProjectId(projectId)
                .setPropertyId(propertyId)
                .build();
        assertThat(outRecords.get(kOut).getLabel()).isEqualTo("label out 10 (from geov project en)");
        var kIn = ProjectFieldLabelKey.newBuilder()
                .setClassId(rangeId)
                .setIsOutgoing(false)
                .setProjectId(projectId)
                .setPropertyId(propertyId)
                .build();
        assertThat(outRecords.get(kIn).getLabel()).isEqualTo("label in 10 (from ontome en)");
    }

    @Test
    void testLabelInEnglishFromGeovDefaultProject() {
        int domainId = 9;
        int propertyId = 10;
        int rangeId = 11;
        int projectId = 20;
        // add project
        var kP = dev.projects.project.Key.newBuilder()
                .setPkEntity(projectId)
                .build();
        var vP = dev.projects.project.Value.newBuilder()
                .setFkLanguage(I.DE.get())
                .build();
        projectTopic.pipeInput(kP, vP);

        // add project property
        var kC = ProjectPropertyKey.newBuilder()
                .setProjectId(projectId)
                .setDomainId(domainId)
                .setPropertyId(propertyId)
                .setRangeId(rangeId)
                .build();
        var vC = ProjectPropertyValue.newBuilder()
                .setProjectId(projectId)
                .setDomainId(domainId)
                .setPropertyId(propertyId)
                .setRangeId(rangeId)
                .build();
        projectPropertyTopic.pipeInput(kC, vC);

        // add ontome property label in english
        var kO = OntomePropertyLabelKey.newBuilder()
                .setPropertyId(propertyId)
                .setLanguageId(I.EN.get())
                .build();
        var vO = OntomePropertyLabelValue.newBuilder()
                .setPropertyId(propertyId)
                .setLanguageId(I.EN.get())
                .setLabel("label out 10 (from ontome en)")
                .setInverseLabel("label in 10 (from ontome en)")
                .build();
        ontomePropertyLabelTopic.pipeInput(kO, vO);

        // add geov default property out label in english
        var kG = GeovPropertyLabelKey.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.EN.get())
                .build();
        var vG = GeovPropertyLabelValue.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setClassId(domainId)
                .setPropertyId(propertyId)
                .setIsOutgoing(true)
                .setLanguageId(I.EN.get())
                .setLabel("label out 10 (from geov default en)")
                .build();
        geovPropertyLabelTopic.pipeInput(kG, vG);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);
        var kOut = ProjectFieldLabelKey.newBuilder()
                .setClassId(domainId)
                .setIsOutgoing(true)
                .setProjectId(projectId)
                .setPropertyId(propertyId)
                .build();
        assertThat(outRecords.get(kOut).getLabel()).isEqualTo("label out 10 (from geov default en)");
        var kIn = ProjectFieldLabelKey.newBuilder()
                .setClassId(rangeId)
                .setIsOutgoing(false)
                .setProjectId(projectId)
                .setPropertyId(propertyId)
                .build();
        assertThat(outRecords.get(kIn).getLabel()).isEqualTo("label in 10 (from ontome en)");
    }


    @Test
    void testLabelInEnglishFromOntome() {
        int domainId = 9;
        int propertyId = 10;
        int rangeId = 11;
        int projectId = 20;
        // add project
        var kP = dev.projects.project.Key.newBuilder()
                .setPkEntity(projectId)
                .build();
        var vP = dev.projects.project.Value.newBuilder()
                .setFkLanguage(I.DE.get())
                .build();
        projectTopic.pipeInput(kP, vP);

        // add project property
        var kC = ProjectPropertyKey.newBuilder()
                .setProjectId(projectId)
                .setDomainId(domainId)
                .setPropertyId(propertyId)
                .setRangeId(rangeId)
                .build();
        var vC = ProjectPropertyValue.newBuilder()
                .setProjectId(projectId)
                .setDomainId(domainId)
                .setPropertyId(propertyId)
                .setRangeId(rangeId)
                .build();
        projectPropertyTopic.pipeInput(kC, vC);

        // add ontome property label in english
        var kO = OntomePropertyLabelKey.newBuilder()
                .setPropertyId(propertyId)
                .setLanguageId(I.EN.get())
                .build();
        var vO = OntomePropertyLabelValue.newBuilder()
                .setPropertyId(propertyId)
                .setLanguageId(I.EN.get())
                .setLabel("label out 10 (from ontome en)")
                .setInverseLabel("label in 10 (from ontome en)")
                .build();
        ontomePropertyLabelTopic.pipeInput(kO, vO);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);
        var kOut = ProjectFieldLabelKey.newBuilder()
                .setClassId(domainId)
                .setIsOutgoing(true)
                .setProjectId(projectId)
                .setPropertyId(propertyId)
                .build();
        assertThat(outRecords.get(kOut).getLabel()).isEqualTo("label out 10 (from ontome en)");
        var kIn = ProjectFieldLabelKey.newBuilder()
                .setClassId(rangeId)
                .setIsOutgoing(false)
                .setProjectId(projectId)
                .setPropertyId(propertyId)
                .build();
        assertThat(outRecords.get(kIn).getLabel()).isEqualTo("label in 10 (from ontome en)");
    }

}
