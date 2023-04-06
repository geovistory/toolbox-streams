package org.geovistory.toolbox.streams.field.changes.processors;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.FieldChangeKey;
import org.geovistory.toolbox.streams.avro.FieldChangeValue;
import org.geovistory.toolbox.streams.field.changes.AvroSerdes;
import org.geovistory.toolbox.streams.field.changes.BuilderSingleton;
import org.geovistory.toolbox.streams.field.changes.RegisterInputTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectFieldChangeTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectFieldChangeTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<dev.information.statement.Key, dev.information.statement.Value> infStatementTopic;
    private TestInputTopic<dev.projects.info_proj_rel.Key, dev.projects.info_proj_rel.Value> proInfoProjRelTopic;
    private TestOutputTopic<FieldChangeKey, FieldChangeValue> outputTopic;


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
        var registerInputTopic = new RegisterInputTopic(avroSerdes, builderSingleton);
        var projectFieldChange = new ProjectFieldChange(avroSerdes, registerInputTopic);
        projectFieldChange.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        infStatementTopic = testDriver.createInputTopic(
                projectFieldChange.inputTopicInfStatement(),
                avroSerdes.InfStatementKey().serializer(),
                avroSerdes.InfStatementValue().serializer());

        proInfoProjRelTopic = testDriver.createInputTopic(
                projectFieldChange.inputTopicProInfoProjRel(),
                avroSerdes.ProInfoProjRelKey().serializer(),
                avroSerdes.ProInfoProjRelValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                projectFieldChange.outputTopicProjectFieldChange(),
                avroSerdes.FieldChangeKey().deserializer(),
                avroSerdes.FieldChangeValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testOneStatementAndOneProjectRel() {
        var projectId = 10;
        var statementId = 20;
        var propertyId = 30;
        var t1 = "2020-01-02T12:15:00Z";
        // add relation between project and statement
        var kR = dev.projects.info_proj_rel.Key.newBuilder()
                .setFkEntity(statementId)
                .setFkProject(projectId)
                .build();
        var vR = dev.projects.info_proj_rel.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(1)
                .setFkEntity(statementId)
                .setFkProject(projectId)
                .setTmspLastModification(t1)
                .setIsInProject(true)
                .build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add statement
        var kE = dev.information.statement.Key.newBuilder().setPkEntity(statementId).build();
        var vE = dev.information.statement.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(statementId)
                .setFkProperty(propertyId)
                .setFkSubjectInfo(1)
                .setFkObjectInfo(2)
                .build();
        infStatementTopic.pipeInput(kE, vE);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);
        var resultingKey = FieldChangeKey.newBuilder()
                .setFkProject(projectId)
                .setFkProperty(propertyId)
                .setFkSourceInfo(1)
                .setIsOutgoing(true)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getTmspLastModification().toString()).isEqualTo(t1);
        resultingKey = FieldChangeKey.newBuilder()
                .setFkProject(projectId)
                .setFkProperty(propertyId)
                .setFkSourceInfo(2)
                .setIsOutgoing(false)
                .build();
        record = outRecords.get(resultingKey);
        assertThat(record.getTmspLastModification().toString()).isEqualTo(t1);
    }

    @Test
    void testUpdateProjectRel() {
        var projectId = 10;
        var statementId = 20;
        var propertyId = 30;
        var t1 = "2020-01-02T12:15:00Z";
        var t2 = "2021-01-02T12:15:00Z";
        var t3 = "2022-01-02T12:15:00Z";
        // add relation between project and statement
        var kR = dev.projects.info_proj_rel.Key.newBuilder()
                .setFkEntity(statementId)
                .setFkProject(projectId)
                .build();
        var vR = dev.projects.info_proj_rel.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setEntityVersion(1)
                .setFkEntity(statementId)
                .setFkProject(projectId)
                .setTmspLastModification(t1)
                .setIsInProject(true)
                .build();
        proInfoProjRelTopic.pipeInput(kR, vR);

        // add statement
        var kE = dev.information.statement.Key.newBuilder().setPkEntity(statementId).build();
        var vE = dev.information.statement.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setPkEntity(statementId)
                .setFkProperty(propertyId)
                .setFkSubjectInfo(1)
                .setFkObjectInfo(2)
                .build();
        infStatementTopic.pipeInput(kE, vE);

        // update rel
        vR.setTmspLastModification(t3);
        proInfoProjRelTopic.pipeInput(kR, vR);

        // update rel with old timestamp (should not have effect)
        vR.setTmspLastModification(t2);
        proInfoProjRelTopic.pipeInput(kR, vR);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(2);
        var resultingKey = FieldChangeKey.newBuilder()
                .setFkProject(projectId)
                .setFkProperty(propertyId)
                .setFkSourceInfo(1)
                .setIsOutgoing(true)
                .build();
        var record = outRecords.get(resultingKey);
        assertThat(record.getTmspLastModification().toString()).isEqualTo(t3);
    }


}
