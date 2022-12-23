package org.geovistory.toolbox.streams.topology;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityValue;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.topologies.ProjectEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectEntityTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectEntityTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<dev.information.resource.Key, dev.information.resource.Value> infResourceTopic;
    private TestInputTopic<dev.projects.info_proj_rel.Key, dev.projects.info_proj_rel.Value> proInfoProjRelTopic;
    private TestOutputTopic<ProjectEntityKey, ProjectEntityValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = ProjectEntity.buildStandalone(new StreamsBuilder());

        testDriver = new TopologyTestDriver(topology, props);

        var avroSerdes = new ConfluentAvroSerdes();

        infResourceTopic = testDriver.createInputTopic(
                ProjectEntity.input.TOPICS.inf_resource,
                avroSerdes.InfResourceKey().serializer(),
                avroSerdes.InfResourceValue().serializer());

        proInfoProjRelTopic = testDriver.createInputTopic(
                ProjectEntity.input.TOPICS.pro_info_proj_rel,
                avroSerdes.ProInfoProjRelKey().serializer(),
                avroSerdes.ProInfoProjRelValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                ProjectEntity.output.TOPICS.project_entity,
                avroSerdes.ProjectEntityKey().deserializer(),
                avroSerdes.ProjectEntityValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testOneEntityAndOneProjectRel() {
        var projectId = 10;
        var entityId = 20;
        var classId = 30;
        // add relation between project and entity
        var kR = dev.projects.info_proj_rel.Key.newBuilder()
                .setFkEntity(entityId)
                .setFkProject(projectId)
                .build();
        var vR = dev.projects.info_proj_rel.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(1)
                .setFkEntity(entityId)
                .setFkProject(projectId)
                .setIsInProject(true)
                .build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add entity
        var kE = dev.information.resource.Key.newBuilder().setPkEntity(entityId).build();
        var vE = dev.information.resource.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(entityId)
                .setFkClass(classId)
                .build();
        infResourceTopic.pipeInput(kE, vE);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectEntityKey.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getDeleted$1()).isFalse();
        assertThat(record.getClassId()).isEqualTo(classId);
    }

    @Test
    void testDeleteProjectRel() {
        var projectId = 10;
        var entityId = 20;
        var classId = 30;
        // add relation between project and entity
        var kR = dev.projects.info_proj_rel.Key.newBuilder()
                .setFkEntity(entityId)
                .setFkProject(projectId)
                .build();
        var vR = dev.projects.info_proj_rel.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(1)
                .setFkEntity(entityId)
                .setFkProject(projectId)
                .setIsInProject(true)
                .build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add entity
        var kE = dev.information.resource.Key.newBuilder().setPkEntity(entityId).build();
        var vE = dev.information.resource.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(entityId)
                .setFkClass(classId)
                .build();
        infResourceTopic.pipeInput(kE, vE);

        vR.setDeleted$1("true");
        proInfoProjRelTopic.pipeInput(kR, vR);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectEntityKey.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .build();
        assertThat(outRecords.containsKey(resultingKey)).isTrue();
        var record = outRecords.get(resultingKey);
        assertThat(record).isNull();
    }

    @Test
    void testDeleteEntity() {
        var projectId = 10;
        var entityId = 20;
        var classId = 30;
        // add relation between project and entity
        var kR = dev.projects.info_proj_rel.Key.newBuilder()
                .setFkEntity(entityId)
                .setFkProject(projectId)
                .build();
        var vR = dev.projects.info_proj_rel.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(1)
                .setFkEntity(entityId)
                .setFkProject(projectId)
                .setIsInProject(true)
                .build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add entity
        var kE = dev.information.resource.Key.newBuilder().setPkEntity(entityId).build();
        var vE = dev.information.resource.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(entityId)
                .setFkClass(classId)
                .build();
        infResourceTopic.pipeInput(kE, vE);

        vE.setDeleted$1("true");
        infResourceTopic.pipeInput(kE, vE);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectEntityKey.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .build();
        assertThat(outRecords.containsKey(resultingKey)).isTrue();
        var record = outRecords.get(resultingKey);
        assertThat(record).isNull();
    }

    @Test
    void testRemoveEntityFromProject() {
        var projectId = 10;
        var entityId = 20;
        var classId = 30;
        // add relation between project and entity
        var kR = dev.projects.info_proj_rel.Key.newBuilder()
                .setFkEntity(entityId)
                .setFkProject(projectId)
                .build();
        var vR = dev.projects.info_proj_rel.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(1)
                .setFkEntity(entityId)
                .setFkProject(projectId)
                .setIsInProject(true)
                .build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add entity
        var kE = dev.information.resource.Key.newBuilder().setPkEntity(entityId).build();
        var vE = dev.information.resource.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(entityId)
                .setFkClass(classId)
                .build();
        infResourceTopic.pipeInput(kE, vE);

        vR.setIsInProject(false);
        proInfoProjRelTopic.pipeInput(kR, vR);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var resultingKey = ProjectEntityKey.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId)
                .build();
        assertThat(outRecords.containsKey(resultingKey)).isTrue();
        var record = outRecords.get(resultingKey);
        assertThat(record).isNull();
    }

    @Test
    void testTwoProjectsOneEntity() {
        var projectOneId = 10;
        var projectTwoId = 11;
        var entityId = 20;
        var classId = 30;

        // add relation between project one and entity
        var kR = dev.projects.info_proj_rel.Key.newBuilder()
                .setFkEntity(entityId)
                .setFkProject(projectOneId)
                .build();
        var vR = dev.projects.info_proj_rel.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(1)
                .setFkEntity(entityId)
                .setFkProject(projectOneId)
                .setIsInProject(true)
                .build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add relation between project two and entity
        kR = dev.projects.info_proj_rel.Key.newBuilder()
                .setFkEntity(entityId)
                .setFkProject(projectTwoId)
                .build();
        vR = dev.projects.info_proj_rel.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(1)
                .setFkEntity(entityId)
                .setFkProject(projectTwoId)
                .setIsInProject(true)
                .build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add entity
        var kE = dev.information.resource.Key.newBuilder().setPkEntity(entityId).build();
        var vE = dev.information.resource.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(entityId)
                .setFkClass(classId)
                .build();
        infResourceTopic.pipeInput(kE, vE);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);
        var resultingKeyOne = ProjectEntityKey.newBuilder()
                .setProjectId(projectOneId)
                .setEntityId(entityId)
                .build();
        assertThat(outRecords.containsKey(resultingKeyOne)).isTrue();
        var resultingKeyTwo = ProjectEntityKey.newBuilder()
                .setProjectId(projectTwoId)
                .setEntityId(entityId)
                .build();
        assertThat(outRecords.containsKey(resultingKeyTwo)).isTrue();
    }


}
