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

class CommunityClassLabelTest {

    private static final String SCHEMA_REGISTRY_SCOPE = CommunityClassLabelTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;

    private TestInputTopic<OntomeClassLabelKey, OntomeClassLabelValue> ontomeClassLabelTopic;
    private TestInputTopic<GeovClassLabelKey, GeovClassLabelValue> geovClassLabelTopic;

    private TestOutputTopic<OntomeClassLabelKey, CommunityClassLabelValue> outputTopic;


    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = CommunityClassLabel.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();


        ontomeClassLabelTopic = testDriver.createInputTopic(
                CommunityClassLabel.input.TOPICS.ontome_class_label,
                avroSerdes.OntomeClassLabelKey().serializer(),
                avroSerdes.OntomeClassLabelValue().serializer());

        geovClassLabelTopic = testDriver.createInputTopic(
                CommunityClassLabel.input.TOPICS.geov_class_label,
                avroSerdes.GeovClassLabelKey().serializer(),
                avroSerdes.GeovClassLabelValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                CommunityClassLabel.output.TOPICS.community_class_label,
                avroSerdes.OntomeClassLabelKey().deserializer(),
                avroSerdes.CommunityClassLabelValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testGeovOveridesOntomeLabel() {
        int classId = 10;

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
        var k = OntomeClassLabelKey.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.EN.get())
                .build();
        assertThat(outRecords.get(k).getLabel()).isEqualTo("label 10 (from geov default en)");
    }


    @Test
    void testOnlyOntome() {
        int classId = 10;

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
        var k = OntomeClassLabelKey.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.EN.get())
                .build();
        assertThat(outRecords.get(k).getLabel()).isEqualTo("label 10 (from ontome en)");
    }


    @Test
    void testOnlyGeov() {
        int classId = 10;

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
        var k = OntomeClassLabelKey.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.EN.get())
                .build();
        assertThat(outRecords.get(k).getLabel()).isEqualTo("label 10 (from geov default en)");
    }


    @Test
    void testFilterDefaultProject() {
        int classId = 10;

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
                .setProjectId(123)
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
        var k = OntomeClassLabelKey.newBuilder()
                .setClassId(classId)
                .setLanguageId(I.EN.get())
                .build();
        assertThat(outRecords.get(k).getLabel()).isEqualTo("label 10 (from ontome en)");
    }


}
