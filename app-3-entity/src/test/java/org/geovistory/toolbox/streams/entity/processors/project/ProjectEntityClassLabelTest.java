package org.geovistory.toolbox.streams.entity.processors.project;


import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.kafka.streams.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.I;
import org.geovistory.toolbox.streams.entity.InputTopicNames;
import org.geovistory.toolbox.streams.entity.OutputTopicNames;
import org.geovistory.toolbox.streams.testlib.TopologyTestDriverProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
class ProjectEntityClassLabelTest {

    @Inject
    Topology topology;

    @Inject
    ConfiguredAvroSerde as;

    @Inject
    OutputTopicNames outputTopicNames;
    @Inject
    InputTopicNames inputTopicNames;
    @ConfigProperty(name = "kafka-streams.state.dir")
    public String stateDir;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectEntityKey, ProjectEntityValue> projectEntityTopic;
    private TestInputTopic<ProjectClassLabelKey, ProjectClassLabelValue> projectClassLabelTopic;
    private TestOutputTopic<ProjectEntityKey, ProjectEntityClassLabelValue> outputTopic;

    @BeforeEach
    void setup() {

        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");

        testDriver = new TopologyTestDriver(topology, props);

        projectClassLabelTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectClassLabel(),
                avroSerdes.ProjectClassLabelKey().serializer(),
                avroSerdes.ProjectClassLabelValue().serializer());

        projectEntityTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectEntity(),
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectEntityClassLabel(),
                avroSerdes.ProjectEntityKey().deserializer(),
                avroSerdes.ProjectEntityClassLabelValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }


    @Test
    void testProjectEntityClassLabel() {

        var entityId = "i1";
        var projectId = 2;
        var classId = 3;
        var classLabel = "my_class";

        // add an entity
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(projectId).build();
        var vE = ProjectEntityValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setClassId(3).build();
        projectEntityTopic.pipeInput(kE, vE);

        // add a class label
        var kS = ProjectClassLabelKey.newBuilder().setProjectId(projectId).setClassId(classId).build();
        var vS = ProjectClassLabelValue.newBuilder().setProjectId(projectId).setClassId(classId)
                .setLanguageId(I.EN.get())
                .setLabel(classLabel).build();
        projectClassLabelTopic.pipeInput(kS, vS);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kE);
        assertThat(record.getClassLabel()).isEqualTo(classLabel);

    }

    @Test
    void testDeleteClassLabel() {

        var entityId = "i1";
        var projectId = 2;
        var classId = 3;
        var classLabel = "my_class";

        // add an entity
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(projectId).build();
        var vE = ProjectEntityValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setClassId(3).build();
        projectEntityTopic.pipeInput(kE, vE);

        // add a class label
        var kS = ProjectClassLabelKey.newBuilder().setProjectId(projectId).setClassId(classId).build();
        var vS = ProjectClassLabelValue.newBuilder().setProjectId(projectId).setClassId(classId)
                .setLanguageId(I.EN.get())
                .setLabel(classLabel).build();
        projectClassLabelTopic.pipeInput(kS, vS);
        vS.setDeleted$1(true);
        projectClassLabelTopic.pipeInput(kS, vS);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kE);
        assertThat(record.getDeleted$1()).isEqualTo(true);

    }

    @Test
    void testDeleteEntity() {

        var entityId = "i1";
        var projectId = 2;
        var classId = 3;
        var classLabel = "my_class";

        // add an entity
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(projectId).build();
        var vE = ProjectEntityValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setClassId(3).build();
        projectEntityTopic.pipeInput(kE, vE);
        vE.setDeleted$1(true);
        projectEntityTopic.pipeInput(kE, vE);

        // add a class label
        var kS = ProjectClassLabelKey.newBuilder().setProjectId(projectId).setClassId(classId).build();
        var vS = ProjectClassLabelValue.newBuilder().setProjectId(projectId).setClassId(classId)
                .setLanguageId(I.EN.get())
                .setLabel(classLabel).build();
        projectClassLabelTopic.pipeInput(kS, vS);


        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kE);
        assertThat(record.getDeleted$1()).isEqualTo(true);

    }
}
