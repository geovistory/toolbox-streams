package org.geovistory.toolbox.streams.rdf.processors.project;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.rdf.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectCustomRdfsLabelsTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectCustomRdfsLabelsTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<dev.projects.project.Key, dev.projects.project.Value> projectTopic;
    private TestOutputTopic<ProjectRdfKey, ProjectRdfValue> outputTopic;

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
        var projectCustomRdfsLabels = new ProjectCustomRdfsLabels(avroSerdes, registerInputTopic, outputTopicNames);
        projectCustomRdfsLabels.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        projectTopic = testDriver.createInputTopic(
                inputTopicNames.getProject(),
                avroSerdes.ProProjectKey().serializer(),
                avroSerdes.ProProjectValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectRdf(),
                avroSerdes.ProjectRdfKey().deserializer(),
                avroSerdes.ProjectRdfValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    /**
     * The goal of this test is to check if the output is not empty
     */
    @Test
    void testLanguageOutputIsNotEmpty() {
        var projectId = 1;

        var k = dev.projects.project.Key.newBuilder()
                .setPkEntity(projectId)
                .build();
        var v = dev.projects.project.Value.newBuilder()
                .setFkLanguage(I.EN.get())
                .build();

        /*var k = ProjectRdfKey.newBuilder().setProjectId(projectId).setTurtle("").build();
        var v = ProjectRdfValue.newBuilder()
                .setOperation(Operation.insert)
                .build();*/
        projectTopic.pipeInput(k, v);

        assertThat(outputTopic.isEmpty()).isFalse();
    }

    /**
     * The goal of this test is to check if the output has the correct size
     * The size should be 18, the number of RDF triples to be added to a new project dataset
     */
    @Test
    void testSizeOfLanguageOutput() {
        var projectId = 1;

        var k = dev.projects.project.Key.newBuilder()
                .setPkEntity(projectId)
                .build();
        var v = dev.projects.project.Value.newBuilder()
                .setFkLanguage(I.EN.get())
                .build();
        projectTopic.pipeInput(k, v);

        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(18);
    }

    /**
     * The goal of this test is to check if all the 18 expected triples are added to the output topic with operation = insert
     */
    @Test
    void testTurtleAndOperationValue() {
        var projectId = 1;

        var k = dev.projects.project.Key.newBuilder()
                .setPkEntity(projectId)
                .build();
        var v = dev.projects.project.Value.newBuilder()
                .setFkLanguage(I.EN.get())
                .build();
        projectTopic.pipeInput(k, v);

        var outRecords = outputTopic.readKeyValuesToMap();

        var expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/01/rdf-schema#label> \"has type\"@en .")
                .build();

        var record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://www.w3.org/2000/01/rdf-schema#label> <http://www.w3.org/2000/01/rdf-schema#label> \"has label\"@en .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://www.w3.org/2002/07/owl#sameAs> <http://www.w3.org/2000/01/rdf-schema#label> \"same as\"@en .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/c50> <http://www.w3.org/2000/01/rdf-schema#label> \"Time-Span\"@en .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/c50> a <http://www.w3.org/2002/07/owl#Class> .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/p4> <http://www.w3.org/2000/01/rdf-schema#label> \"has time-span\"@en .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://ontome.net/ontology/p4i> <http://www.w3.org/2000/01/rdf-schema#label> \"is time-span of\"@en .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://www.w3.org/2006/time#DateTimeDescription> <http://www.w3.org/2000/01/rdf-schema#label> \"Date-Time description\"@en .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://www.w3.org/2006/time#hasTRS> <http://www.w3.org/2000/01/rdf-schema#label> \"Temporal reference system used\"@en .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://www.w3.org/2006/time#unitType> <http://www.w3.org/2000/01/rdf-schema#label> \"temporal unit type\"@en .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://www.w3.org/2006/time#year> <http://www.w3.org/2000/01/rdf-schema#label> \"Year\"@en .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://www.w3.org/2006/time#month> <http://www.w3.org/2000/01/rdf-schema#label> \"Month\"@en .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://www.w3.org/2006/time#day> <http://www.w3.org/2000/01/rdf-schema#label> \"Day\"@en .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://www.w3.org/2006/time#unitYear> <http://www.w3.org/2000/01/rdf-schema#label> \"Year (unit of temporal duration)\"@en .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://www.w3.org/2006/time#unitMonth> <http://www.w3.org/2000/01/rdf-schema#label> \"Month (unit of temporal duration)\"@en .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://www.w3.org/2006/time#unitDay> <http://www.w3.org/2000/01/rdf-schema#label> \"Day (unit of temporal duration)\"@en .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<http://www.opengis.net/def/uom/ISO-8601/0/Gregorian> <http://www.w3.org/2000/01/rdf-schema#label> \"Gregorian Calendar\"@en .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);

        expectedKey = ProjectRdfKey.newBuilder()
                .setProjectId(projectId)
                .setTurtle("<https://d-nb.info/gnd/4318310-4> <http://www.w3.org/2000/01/rdf-schema#label> \"Julian Calendar\"@en .")
                .build();

        record = outRecords.get(expectedKey);
        assertThat(record.getOperation()).isEqualTo(Operation.insert);
    }
}
