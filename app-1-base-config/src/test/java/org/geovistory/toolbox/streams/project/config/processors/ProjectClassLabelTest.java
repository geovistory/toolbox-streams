package org.geovistory.toolbox.streams.project.config.processors;

import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.project.config.I;
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
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = ProjectClassLabel.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();


        ontomeClassLabelTopic = testDriver.createInputTopic(
                ProjectClassLabel.input.TOPICS.ontome_class_label,
                avroSerdes.OntomeClassLabelKey().serializer(),
                avroSerdes.OntomeClassLabelValue().serializer());

        geovClassLabelTopic = testDriver.createInputTopic(
                ProjectClassLabel.input.TOPICS.geov_class_label,
                avroSerdes.GeovClassLabelKey().serializer(),
                avroSerdes.GeovClassLabelValue().serializer());

        projectClassTopic = testDriver.createInputTopic(
                ProjectClassLabel.input.TOPICS.project_class,
                avroSerdes.ProjectClassKey().serializer(),
                avroSerdes.ProjectClassValue().serializer());

        projectTopic = testDriver.createInputTopic(
                ProjectClassLabel.input.TOPICS.project,
                avroSerdes.ProProjectKey().serializer(),
                avroSerdes.ProProjectValue().serializer());


        outputTopic = testDriver.createOutputTopic(
                ProjectClassLabel.output.TOPICS.project_class_label,
                avroSerdes.ProjectClassLabelKey().deserializer(),
                avroSerdes.ProjectClassLabelValue().deserializer());
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
