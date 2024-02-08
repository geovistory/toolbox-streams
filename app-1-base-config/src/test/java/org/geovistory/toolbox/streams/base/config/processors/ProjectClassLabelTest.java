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

class ProjectClassLabelTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectClassLabelTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;

    private TestInputTopic<OntomeClassLabelKey, OntomeClassLabelValue> ontomeClassLabelTopic;
    private TestInputTopic<GeovClassLabelKey, GeovClassLabelValue> geovClassLabelTopic;
    private TestInputTopic<ProjectClassKey, ProjectClassValue> projectClassTopic;
    private TestInputTopic<dev.projects.project.Key, dev.projects.project.Value> projectTopic;
    private TestOutputTopic<ProjectClassLabelKey, ProjectClassLabelValue> outputTopic;


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
        var projectClassLabel = new ProjectClassLabel(as, registerInputTopic, registerInnerTopic, outputTopicNames);
        projectClassLabel.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);


        ontomeClassLabelTopic = testDriver.createInputTopic(
                inputTopicNames.ontomeClassLabel(),
                as.<OntomeClassLabelKey>key().serializer(),
                as.<OntomeClassLabelValue>value().serializer());

        geovClassLabelTopic = testDriver.createInputTopic(
                outputTopicNames.geovClassLabel(),
                as.<GeovClassLabelKey>key().serializer(),
                as.<GeovClassLabelValue>value().serializer());

        projectClassTopic = testDriver.createInputTopic(
                outputTopicNames.projectClass(),
                as.<ProjectClassKey>key().serializer(),
                as.<ProjectClassValue>value().serializer());

        projectTopic = testDriver.createInputTopic(
                inputTopicNames.proProject(),
                as.<dev.projects.project.Key>key().serializer(),
                as.<dev.projects.project.Value>value().serializer());


        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectClassLabel(),
                as.<ProjectClassLabelKey>key().deserializer(),
                as.<ProjectClassLabelValue>value().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testLabelInProjectLangFromGeovProject() {
        int classId = 10;
        int projectId = 20;
        // add project
        var kP = dev.projects.project.Key.newBuilder()
                .setPkEntity(projectId)
                .build();
        var vP = dev.projects.project.Value.newBuilder()
                .setFkLanguage(I.DE.get())
                .build();
        projectTopic.pipeInput(kP, vP);

        // add project class
        var kC = ProjectClassKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .build();
        var vC = ProjectClassValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .build();
        projectClassTopic.pipeInput(kC, vC);

        // add ontome class label in english
        var kO = OntomeClassLabelKey.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.EN.get())
                .build();
        var vO = OntomeClassLabelValue.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.EN.get())
                .setLabel("label 10 (from ontome en)")
                .build();
        ontomeClassLabelTopic.pipeInput(kO, vO);

        // add geov default class label in english
        var kG = GeovClassLabelKey.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setLanguageId(I.EN.get())
                .setClassId(classId)
                .build();
        var vG = GeovClassLabelValue.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setLanguageId(I.EN.get())
                .setClassId(classId)
                .setLabel("label 10 (from geov default en)")
                .build();
        geovClassLabelTopic.pipeInput(kG, vG);

        // add geov project class label in english
        kG = GeovClassLabelKey.newBuilder()
                .setProjectId(projectId)
                .setLanguageId(I.EN.get())
                .setClassId(10)
                .build();
        vG = GeovClassLabelValue.newBuilder()
                .setProjectId(projectId)
                .setLanguageId(I.EN.get())
                .setClassId(10)
                .setLabel("label 10 (from geov project en)")
                .build();
        geovClassLabelTopic.pipeInput(kG, vG);

        // add ontome class label in project lang
        kO = OntomeClassLabelKey.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.DE.get())
                .build();
        vO = OntomeClassLabelValue.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.DE.get())
                .setLabel("label 10 (from ontome de)")
                .build();
        ontomeClassLabelTopic.pipeInput(kO, vO);

        // add geov default class label in project lang
        kG = GeovClassLabelKey.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setLanguageId(I.DE.get())
                .setClassId(classId)
                .build();
        vG = GeovClassLabelValue.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setLanguageId(I.DE.get())
                .setClassId(classId)
                .setLabel("label 10 (from geov default de)")
                .build();
        geovClassLabelTopic.pipeInput(kG, vG);

        // add geov project class label in project lang
        kG = GeovClassLabelKey.newBuilder()
                .setProjectId(projectId)
                .setLanguageId(I.DE.get())
                .setClassId(classId)
                .build();
        vG = GeovClassLabelValue.newBuilder()
                .setProjectId(projectId)
                .setLanguageId(I.DE.get())
                .setClassId(classId)
                .setLabel("label 10 (from geov project de)")
                .build();
        geovClassLabelTopic.pipeInput(kG, vG);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var k = ProjectClassLabelKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .build();
        assertThat(outRecords.get(k).getLabel()).isEqualTo("label 10 (from geov project de)");
    }

    @Test
    void testLabelInProjectLangFromGeovDefaultProject() {
        int classId = 10;
        int projectId = 20;
        // add project
        var kP = dev.projects.project.Key.newBuilder()
                .setPkEntity(projectId)
                .build();
        var vP = dev.projects.project.Value.newBuilder()
                .setFkLanguage(I.DE.get())
                .build();
        projectTopic.pipeInput(kP, vP);

        // add project class
        var kC = ProjectClassKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .build();
        var vC = ProjectClassValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .build();
        projectClassTopic.pipeInput(kC, vC);

        // add ontome class label in english
        var kO = OntomeClassLabelKey.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.EN.get())
                .build();
        var vO = OntomeClassLabelValue.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.EN.get())
                .setLabel("label 10 (from ontome en)")
                .build();
        ontomeClassLabelTopic.pipeInput(kO, vO);

        // add geov default class label in english
        var kG = GeovClassLabelKey.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setLanguageId(I.EN.get())
                .setClassId(classId)
                .build();
        var vG = GeovClassLabelValue.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setLanguageId(I.EN.get())
                .setClassId(classId)
                .setLabel("label 10 (from geov default en)")
                .build();
        geovClassLabelTopic.pipeInput(kG, vG);

        // add geov project class label in english
        kG = GeovClassLabelKey.newBuilder()
                .setProjectId(projectId)
                .setLanguageId(I.EN.get())
                .setClassId(10)
                .build();
        vG = GeovClassLabelValue.newBuilder()
                .setProjectId(projectId)
                .setLanguageId(I.EN.get())
                .setClassId(10)
                .setLabel("label 10 (from geov project en)")
                .build();
        geovClassLabelTopic.pipeInput(kG, vG);

        // add ontome class label in project lang
        kO = OntomeClassLabelKey.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.DE.get())
                .build();
        vO = OntomeClassLabelValue.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.DE.get())
                .setLabel("label 10 (from ontome de)")
                .build();
        ontomeClassLabelTopic.pipeInput(kO, vO);

        // add geov default class label in project lang
        kG = GeovClassLabelKey.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setLanguageId(I.DE.get())
                .setClassId(classId)
                .build();
        vG = GeovClassLabelValue.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setLanguageId(I.DE.get())
                .setClassId(classId)
                .setLabel("label 10 (from geov default de)")
                .build();
        geovClassLabelTopic.pipeInput(kG, vG);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var k = ProjectClassLabelKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .build();
        assertThat(outRecords.get(k).getLabel()).isEqualTo("label 10 (from geov default de)");
    }

    @Test
    void testLabelInProjectLangFromOntome() {
        int classId = 10;
        int projectId = 20;
        // add project
        var kP = dev.projects.project.Key.newBuilder()
                .setPkEntity(projectId)
                .build();
        var vP = dev.projects.project.Value.newBuilder()
                .setFkLanguage(I.DE.get())
                .build();
        projectTopic.pipeInput(kP, vP);

        // add project class
        var kC = ProjectClassKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .build();
        var vC = ProjectClassValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .build();
        projectClassTopic.pipeInput(kC, vC);

        // add ontome class label in english
        var kO = OntomeClassLabelKey.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.EN.get())
                .build();
        var vO = OntomeClassLabelValue.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.EN.get())
                .setLabel("label 10 (from ontome en)")
                .build();
        ontomeClassLabelTopic.pipeInput(kO, vO);

        // add geov default class label in english
        var kG = GeovClassLabelKey.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setLanguageId(I.EN.get())
                .setClassId(classId)
                .build();
        var vG = GeovClassLabelValue.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setLanguageId(I.EN.get())
                .setClassId(classId)
                .setLabel("label 10 (from geov default en)")
                .build();
        geovClassLabelTopic.pipeInput(kG, vG);

        // add geov project class label in english
        kG = GeovClassLabelKey.newBuilder()
                .setProjectId(projectId)
                .setLanguageId(I.EN.get())
                .setClassId(10)
                .build();
        vG = GeovClassLabelValue.newBuilder()
                .setProjectId(projectId)
                .setLanguageId(I.EN.get())
                .setClassId(10)
                .setLabel("label 10 (from geov project en)")
                .build();
        geovClassLabelTopic.pipeInput(kG, vG);

        // add ontome class label in project lang
        kO = OntomeClassLabelKey.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.DE.get())
                .build();
        vO = OntomeClassLabelValue.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.DE.get())
                .setLabel("label 10 (from ontome de)")
                .build();
        ontomeClassLabelTopic.pipeInput(kO, vO);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var k = ProjectClassLabelKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .build();
        assertThat(outRecords.get(k).getLabel()).isEqualTo("label 10 (from ontome de)");
    }

    @Test
    void testLabelInEnglishFromGeovProject() {
        int classId = 10;
        int projectId = 20;
        // add project
        var kP = dev.projects.project.Key.newBuilder()
                .setPkEntity(projectId)
                .build();
        var vP = dev.projects.project.Value.newBuilder()
                .setFkLanguage(I.DE.get())
                .build();
        projectTopic.pipeInput(kP, vP);

        // add project class
        var kC = ProjectClassKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .build();
        var vC = ProjectClassValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .build();
        projectClassTopic.pipeInput(kC, vC);

        // add ontome class label in english
        var kO = OntomeClassLabelKey.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.EN.get())
                .build();
        var vO = OntomeClassLabelValue.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.EN.get())
                .setLabel("label 10 (from ontome en)")
                .build();
        ontomeClassLabelTopic.pipeInput(kO, vO);

        // add geov default class label in english
        var kG = GeovClassLabelKey.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setLanguageId(I.EN.get())
                .setClassId(classId)
                .build();
        var vG = GeovClassLabelValue.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setLanguageId(I.EN.get())
                .setClassId(classId)
                .setLabel("label 10 (from geov default en)")
                .build();
        geovClassLabelTopic.pipeInput(kG, vG);

        // add geov project class label in english
        kG = GeovClassLabelKey.newBuilder()
                .setProjectId(projectId)
                .setLanguageId(I.EN.get())
                .setClassId(10)
                .build();
        vG = GeovClassLabelValue.newBuilder()
                .setProjectId(projectId)
                .setLanguageId(I.EN.get())
                .setClassId(10)
                .setLabel("label 10 (from geov project en)")
                .build();
        geovClassLabelTopic.pipeInput(kG, vG);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var k = ProjectClassLabelKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .build();
        assertThat(outRecords.get(k).getLabel()).isEqualTo("label 10 (from geov project en)");
    }

    @Test
    void testLabelInEnglishFromGeovDefaultProject() {
        int classId = 10;
        int projectId = 20;
        // add project
        var kP = dev.projects.project.Key.newBuilder()
                .setPkEntity(projectId)
                .build();
        var vP = dev.projects.project.Value.newBuilder()
                .setFkLanguage(I.DE.get())
                .build();
        projectTopic.pipeInput(kP, vP);

        // add project class
        var kC = ProjectClassKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .build();
        var vC = ProjectClassValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .build();
        projectClassTopic.pipeInput(kC, vC);

        // add ontome class label in english
        var kO = OntomeClassLabelKey.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.EN.get())
                .build();
        var vO = OntomeClassLabelValue.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.EN.get())
                .setLabel("label 10 (from ontome en)")
                .build();
        ontomeClassLabelTopic.pipeInput(kO, vO);

        // add geov default class label in english
        var kG = GeovClassLabelKey.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setLanguageId(I.EN.get())
                .setClassId(classId)
                .build();
        var vG = GeovClassLabelValue.newBuilder()
                .setProjectId(I.DEFAULT_PROJECT.get())
                .setLanguageId(I.EN.get())
                .setClassId(classId)
                .setLabel("label 10 (from geov default en)")
                .build();
        geovClassLabelTopic.pipeInput(kG, vG);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var k = ProjectClassLabelKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .build();
        assertThat(outRecords.get(k).getLabel()).isEqualTo("label 10 (from geov default en)");
    }


    @Test
    void testLabelInEnglishFromOntome() {
        int classId = 10;
        int projectId = 20;
        // add project
        var kP = dev.projects.project.Key.newBuilder()
                .setPkEntity(projectId)
                .build();
        var vP = dev.projects.project.Value.newBuilder()
                .setFkLanguage(I.DE.get())
                .build();
        projectTopic.pipeInput(kP, vP);

        // add project class
        var kC = ProjectClassKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .build();
        var vC = ProjectClassValue.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .build();
        projectClassTopic.pipeInput(kC, vC);

        // add ontome class label in english
        var kO = OntomeClassLabelKey.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.EN.get())
                .build();
        var vO = OntomeClassLabelValue.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.EN.get())
                .setLabel("label 10 (from ontome en)")
                .build();
        ontomeClassLabelTopic.pipeInput(kO, vO);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var k = ProjectClassLabelKey.newBuilder()
                .setProjectId(projectId)
                .setClassId(classId)
                .build();
        assertThat(outRecords.get(k).getLabel()).isEqualTo("label 10 (from ontome en)");
    }

}
