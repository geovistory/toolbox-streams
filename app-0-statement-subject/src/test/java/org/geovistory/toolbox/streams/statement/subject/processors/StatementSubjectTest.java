package org.geovistory.toolbox.streams.statement.subject.processors;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.Entity;
import org.geovistory.toolbox.streams.avro.NodeKey;
import org.geovistory.toolbox.streams.avro.NodeValue;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.statement.subject.AvroSerdes;
import org.geovistory.toolbox.streams.statement.subject.BuilderSingleton;
import org.geovistory.toolbox.streams.statement.subject.RegisterInputTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class StatementSubjectTest {

    private static final String SCHEMA_REGISTRY_SCOPE = StatementSubjectTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ts.information.statement.Key, ts.information.statement.Value> infStatementTopic;
    private TestInputTopic<NodeKey, NodeValue> nodeTopic;
    private TestOutputTopic<ts.information.statement.Key, StatementEnrichedValue> statementWithSubjectTopic;


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
        var statementSubject = new StatementSubject(avroSerdes, registerInputTopic);
        statementSubject.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        infStatementTopic = testDriver.createInputTopic(
                statementSubject.inInfStatement(),
                avroSerdes.InfStatementKey().serializer(),
                avroSerdes.InfStatementValue().serializer()
        );
        nodeTopic = testDriver.createInputTopic(
                statementSubject.inNodes(),
                avroSerdes.NodeKey().serializer(),
                avroSerdes.NodeValue().serializer()
        );
        statementWithSubjectTopic = testDriver.createOutputTopic(
                statementSubject.outStatementWithSubject(),
                avroSerdes.InfStatementKey().deserializer(),
                avroSerdes.StatementEnrichedValue().deserializer());


    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testStatementWithoutSubject() {
        int subjectId = 10;
        int propertyId = 20;
        int objectId = 30;

        // add statement
        var kS = ts.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = ts.information.statement.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setFkSubjectInfo(subjectId)
                .setFkProperty(propertyId)
                .setFkObjectInfo(objectId)
                .build();
        infStatementTopic.pipeInput(kS, vS);

        assertThat(statementWithSubjectTopic.isEmpty()).isTrue();

    }

    @Test
    void testStatementWithEntity() {
        int subjectId = 10;
        int propertyId = 20;
        int objectId = 30;
        int classId = 40;

        // add statement
        var kS = ts.information.statement.Key.newBuilder()
                .setPkEntity(1)
                .build();
        var vS = ts.information.statement.Value.newBuilder()
                .setSchemaName("")
                .setTableName("")
                .setFkSubjectInfo(subjectId)
                .setFkProperty(propertyId)
                .setFkObjectInfo(objectId)
                .build();
        infStatementTopic.pipeInput(kS, vS);

        // add subject entity
        var k2 = NodeKey.newBuilder().setId("i" + subjectId).build();
        var v2 = NodeValue.newBuilder()
                .setClassId(classId)
                .setEntity(Entity.newBuilder()
                        .setFkClass(classId)
                        .setPkEntity(subjectId)
                        .setCommunityVisibilityToolbox(true)
                        .setCommunityVisibilityDataApi(false)
                        .setCommunityVisibilityWebsite(true)
                        .build())
                .build();
        nodeTopic.pipeInput(k2, v2);


        assertThat(statementWithSubjectTopic.isEmpty()).isFalse();
        var outRecords = statementWithSubjectTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);
        var record = outRecords.get(kS);
        assertThat(record.getSubjectClassId()).isEqualTo(classId);
        assertThat(record.getSubject().getEntity().getCommunityVisibilityToolbox()).isEqualTo(true);
        assertThat(record.getSubject().getEntity().getCommunityVisibilityDataApi()).isEqualTo(false);
        assertThat(record.getSubject().getEntity().getCommunityVisibilityWebsite()).isEqualTo(true);
    }

}
