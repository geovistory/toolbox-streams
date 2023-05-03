package org.geovistory.toolbox.streams.entity.preview.processors.project;


import org.apache.kafka.streams.*;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.preview.AvroSerdes;
import org.geovistory.toolbox.streams.entity.preview.InputTopicNames;
import org.geovistory.toolbox.streams.entity.preview.OutputTopicNames;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProjectEntityPreviewTest {

    private static final String SCHEMA_REGISTRY_SCOPE = ProjectEntityPreviewTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<ProjectEntityKey, ProjectEntityValue> projectEntityTopic;
    private TestInputTopic<ProjectEntityKey, ProjectEntityLabelValue> projectEntityLabelTopic;
    private TestInputTopic<ProjectEntityKey, ProjectEntityClassLabelValue> projectEntityClassLabelTopic;
    private TestInputTopic<ProjectEntityKey, ProjectEntityTypeValue> projectEntityTypeTopic;
    private TestInputTopic<ProjectEntityKey, TimeSpanValue> projectEntityTimeSpanTopic;
    private TestInputTopic<ProjectEntityKey, ProjectEntityFulltextValue> projectEntityFulltextTopic;
    private TestInputTopic<ProjectEntityKey, ProjectEntityClassMetadataValue> projectEntityClassMetadataTopic;
    private TestOutputTopic<ProjectEntityKey, EntityPreviewValue> outputTopic;

    @BeforeEach
    void setup() {


        Properties props = new Properties();
        var appId = "test";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        var avroSerdes = new AvroSerdes();
        avroSerdes.QUARKUS_KAFKA_STREAMS_SCHEMA_REGISTRY_URL = MOCK_SCHEMA_REGISTRY_URL;
        var inputTopicNames = new InputTopicNames();
        var outputTopicNames = new OutputTopicNames();
        var projectEntityPreview2 = new ProjectEntityPreview(avroSerdes, inputTopicNames, outputTopicNames);
        var topology = new Topology();
        projectEntityPreview2.addProcessors(topology);
        testDriver = new TopologyTestDriver(topology, props);

        projectEntityTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectEntity(),
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityValue().serializer());

        projectEntityLabelTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectEntityLabel(),
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityLabelValue().serializer());

        projectEntityClassLabelTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectEntityClassLabel(),
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityClassLabelValue().serializer());

        projectEntityTypeTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectEntityType(),
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityTypeValue().serializer());

        projectEntityTimeSpanTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectEntityTimeSpan(),
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.TimeSpanValue().serializer());

        projectEntityFulltextTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectEntityFulltext(),
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityFulltextValue().serializer());

        projectEntityClassMetadataTopic = testDriver.createInputTopic(
                inputTopicNames.getProjectEntityClassMetadata(),
                avroSerdes.ProjectEntityKey().serializer(),
                avroSerdes.ProjectEntityClassMetadataValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.projectEntityPreview(),
                avroSerdes.ProjectEntityKey().deserializer(),
                avroSerdes.EntityPreviewValue().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }


    @Test
    void testJoinAll() {

        var entityId = "i1";
        var pkEntity = 1;
        var projectId = 2;
        var classId = 3;
        var entityLabel = "Foo";
        var entityClassLabel = "Foo Class";
        var entityTypeId = "i2";
        var fkType = 2;
        var entityTypeLabel = "Foo Type";
        var entityFulltext = "Fulltext";
        var entityFirstSecond = 123L;
        var entityLastSecond = 456L;
        var entityTimeSpan = TimeSpan.newBuilder().setP81(
                NewTimePrimitive.newBuilder()
                        .setJulianDay(1)
                        .setCalendar("julian")
                        .setDuration("1 day").build()
        ).build();
        var entityTimeSpanJson = "{\"p81\": {\"julianDay\": 1, \"duration\": \"1 day\", \"calendar\": \"julian\"}, \"p82\": null, \"p81a\": null, \"p81b\": null, \"p82a\": null, \"p82b\": null}";
        var parentClasses = List.of(1, 2, 3);
        var ancestorClasses = List.of(4, 5, 6);
        var parentClassesJson = "[1, 2, 3]";
        var ancestorClassesJson = "[4, 5, 6]";


        // add an entity
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(projectId).build();
        var vE = ProjectEntityValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setClassId(classId).build();
        projectEntityTopic.pipeInput(kE, vE);

        // add an entity label
        var vEL = ProjectEntityLabelValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setLabel(entityLabel).setLabelSlots(List.of(entityLabel)).build();
        projectEntityLabelTopic.pipeInput(kE, vEL);

        // add an entity class label
        var vECL = ProjectEntityClassLabelValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setClassLabel(entityClassLabel).build();
        projectEntityClassLabelTopic.pipeInput(kE, vECL);

        // add an entity type
        var vET = ProjectEntityTypeValue.newBuilder().setEntityId(entityId).setProjectId(projectId)
                .setTypeId(entityTypeId).setTypeLabel(entityTypeLabel).build();
        projectEntityTypeTopic.pipeInput(kE, vET);

        // add an entity time span
        var vETSP = TimeSpanValue.newBuilder().setTimeSpan(entityTimeSpan).setFirstSecond(entityFirstSecond).setLastSecond(entityLastSecond).build();
        projectEntityTimeSpanTopic.pipeInput(kE, vETSP);

        // add an entity fulltext
        var vEFT = ProjectEntityFulltextValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setFulltext(entityFulltext).build();
        projectEntityFulltextTopic.pipeInput(kE, vEFT);

        // add an entity class metadata
        var vECM = ProjectEntityClassMetadataValue.newBuilder().setParentClasses(parentClasses).setAncestorClasses(ancestorClasses).build();
        projectEntityClassMetadataTopic.pipeInput(kE, vECM);
        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var record = outRecords.get(kE);
        assertThat(record.getFkProject()).isEqualTo(projectId);
        assertThat(record.getProject()).isEqualTo(projectId);
        assertThat(record.getEntityId()).isEqualTo(entityId);
        assertThat(record.getPkEntity()).isEqualTo(pkEntity);
        assertThat(record.getFkClass()).isEqualTo(classId);
        assertThat(record.getTypeId()).isEqualTo(entityTypeId);
        assertThat(record.getFkType()).isEqualTo(fkType);
        assertThat(record.getEntityLabel()).isEqualTo(entityLabel);
        assertThat(record.getClassLabel()).isEqualTo(entityClassLabel);
        assertThat(record.getFullText()).isEqualTo(entityFulltext);
        assertThat(record.getTimeSpan()).isEqualTo(entityTimeSpanJson);
        assertThat(record.getFirstSecond()).isEqualTo(entityFirstSecond);
        assertThat(record.getLastSecond()).isEqualTo(entityLastSecond);
        assertThat(record.getParentClasses()).isEqualTo(parentClassesJson);
        assertThat(record.getAncestorClasses()).isEqualTo(ancestorClassesJson);
        assertThat(record.getEntityType()).isEqualTo("teEn");

    }

    @Test
    void testJoinEntityType() {

        var entityId = "i1";
        var pkEntity = 1;
        var projectId = 2;
        var classId = 3;
        var parentClasses = List.of(1, 70, 3);
        var ancestorClasses = List.of(4, 5, 6);
        var parentClassesJson = "[1, 70, 3]";
        var ancestorClassesJson = "[4, 5, 6]";

        // add an entity
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(projectId).build();
        var vE = ProjectEntityValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setClassId(classId).build();
        projectEntityTopic.pipeInput(kE, vE);


        // add an entity class metadata
        var vECM = ProjectEntityClassMetadataValue.newBuilder().setParentClasses(parentClasses).setAncestorClasses(ancestorClasses).build();
        projectEntityClassMetadataTopic.pipeInput(kE, vECM);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var record = outRecords.get(kE);
        assertThat(record.getFkProject()).isEqualTo(projectId);
        assertThat(record.getProject()).isEqualTo(projectId);
        assertThat(record.getEntityId()).isEqualTo(entityId);
        assertThat(record.getPkEntity()).isEqualTo(pkEntity);
        assertThat(record.getFkClass()).isEqualTo(classId);
        assertThat(record.getParentClasses()).isEqualTo(parentClassesJson);
        assertThat(record.getAncestorClasses()).isEqualTo(ancestorClassesJson);
        assertThat(record.getEntityType()).isEqualTo("peIt");

    }

    @Test
    void testJoinTypeWithoutTypeId() {

        var entityId = "i1";
        var projectId = 2;
        var classId = 3;

        // add an entity
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(projectId).build();
        var vE = ProjectEntityValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setClassId(classId).build();
        projectEntityTopic.pipeInput(kE, vE);

        // add an entity type
        var vET = ProjectEntityTypeValue.newBuilder().setEntityId(entityId).setProjectId(projectId)
                .setTypeId("").setTypeLabel("").build();
        projectEntityTypeTopic.pipeInput(kE, vET);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var record = outRecords.get(kE);
        assertThat(record.getFkProject()).isEqualTo(projectId);
        assertThat(record.getProject()).isEqualTo(projectId);
        assertThat(record.getEntityId()).isEqualTo(entityId);
        assertThat(record.getTypeId()).isEqualTo(null);
        assertThat(record.getFkType()).isEqualTo(null);


    }

    @Test
    void testDeleteEntityPreview() {

        var entityId = "i1";
        var projectId = 2;
        var classId = 3;
        var entityLabel = "Foo";
        var entityClassLabel = "Foo Class";
        var entityTypeId = "i2";
        var entityTypeLabel = "Foo Type";
        var entityFulltext = "Fulltext";
        var entityFirstSecond = 123L;
        var entityLastSecond = 456L;
        var entityTimeSpan = TimeSpan.newBuilder().setP81(
                NewTimePrimitive.newBuilder()
                        .setJulianDay(1)
                        .setCalendar("julian")
                        .setDuration("1 day").build()
        ).build();
        var parentClasses = List.of(1, 2, 3);
        var ancestorClasses = List.of(4, 5, 6);

        // add an entity
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(projectId).build();
        var vE = ProjectEntityValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setClassId(classId).build();
        projectEntityTopic.pipeInput(kE, vE);

        // add an entity label
        var vEL = ProjectEntityLabelValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setLabel(entityLabel).setLabelSlots(List.of(entityLabel)).build();
        projectEntityLabelTopic.pipeInput(kE, vEL);

        // add an entity class label
        var vECL = ProjectEntityClassLabelValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setClassLabel(entityClassLabel).build();
        projectEntityClassLabelTopic.pipeInput(kE, vECL);

        // add an entity type
        var vET = ProjectEntityTypeValue.newBuilder().setEntityId(entityId).setProjectId(projectId)
                .setTypeId(entityTypeId).setTypeLabel(entityTypeLabel).build();
        projectEntityTypeTopic.pipeInput(kE, vET);

        // add an entity time span
        var vETSP = TimeSpanValue.newBuilder().setTimeSpan(entityTimeSpan).setFirstSecond(entityFirstSecond).setLastSecond(entityLastSecond).build();
        projectEntityTimeSpanTopic.pipeInput(kE, vETSP);

        // add an entity fulltext
        var vEFT = ProjectEntityFulltextValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setFulltext(entityFulltext).build();
        projectEntityFulltextTopic.pipeInput(kE, vEFT);

        // add an entity class metadata
        var vECM = ProjectEntityClassMetadataValue.newBuilder().setParentClasses(parentClasses).setAncestorClasses(ancestorClasses).build();
        projectEntityClassMetadataTopic.pipeInput(kE, vECM);

        // delete entity
        vE.setDeleted$1(true);
        projectEntityTopic.pipeInput(kE, vE);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var record = outRecords.get(kE);
        assertThat(record).isNull();
    }


    @Test
    void testUpdateFulltext() {

        var entityId = "i1";
        var pkEntity = 1;
        var projectId = 2;
        var classId = 3;
        var entityLabel = "Foo";
        var entityClassLabel = "Foo Class";
        var entityTypeId = "i2";
        var fkType = 2;
        var entityTypeLabel = "Foo Type";
        var entityFulltext = "Fulltext";
        var entityFirstSecond = 123L;
        var entityLastSecond = 456L;
        var entityTimeSpan = TimeSpan.newBuilder().setP81(
                NewTimePrimitive.newBuilder()
                        .setJulianDay(1)
                        .setCalendar("julian")
                        .setDuration("1 day").build()
        ).build();
        var entityTimeSpanJson = "{\"p81\": {\"julianDay\": 1, \"duration\": \"1 day\", \"calendar\": \"julian\"}, \"p82\": null, \"p81a\": null, \"p81b\": null, \"p82a\": null, \"p82b\": null}";
        var parentClasses = List.of(1, 2, 3);
        var ancestorClasses = List.of(4, 5, 6);
        var parentClassesJson = "[1, 2, 3]";
        var ancestorClassesJson = "[4, 5, 6]";


        // add an entity
        var kE = ProjectEntityKey.newBuilder().setEntityId(entityId).setProjectId(projectId).build();
        var vE = ProjectEntityValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setClassId(classId).build();
        projectEntityTopic.pipeInput(kE, vE);

        // add an entity label
        var vEL = ProjectEntityLabelValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setLabel(entityLabel).setLabelSlots(List.of(entityLabel)).build();
        projectEntityLabelTopic.pipeInput(kE, vEL);

        // add an entity class label
        var vECL = ProjectEntityClassLabelValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setClassLabel(entityClassLabel).build();
        projectEntityClassLabelTopic.pipeInput(kE, vECL);

        // add an entity type
        var vET = ProjectEntityTypeValue.newBuilder().setEntityId(entityId).setProjectId(projectId)
                .setTypeId(entityTypeId).setTypeLabel(entityTypeLabel).build();
        projectEntityTypeTopic.pipeInput(kE, vET);

        // add an entity time span
        var vETSP = TimeSpanValue.newBuilder().setTimeSpan(entityTimeSpan).setFirstSecond(entityFirstSecond).setLastSecond(entityLastSecond).build();
        projectEntityTimeSpanTopic.pipeInput(kE, vETSP);

        // add an entity fulltext
        var vEFT = ProjectEntityFulltextValue.newBuilder().setEntityId(entityId).setProjectId(projectId).setFulltext(entityFulltext).build();
        projectEntityFulltextTopic.pipeInput(kE, vEFT);

        vEFT.setFulltext("Fulltext2");
        projectEntityFulltextTopic.pipeInput(kE, vEFT);

        // add an entity class metadata
        var vECM = ProjectEntityClassMetadataValue.newBuilder().setParentClasses(parentClasses).setAncestorClasses(ancestorClasses).build();
        projectEntityClassMetadataTopic.pipeInput(kE, vECM);
        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var record = outRecords.get(kE);
        assertThat(record.getFkProject()).isEqualTo(projectId);
        assertThat(record.getProject()).isEqualTo(projectId);
        assertThat(record.getEntityId()).isEqualTo(entityId);
        assertThat(record.getPkEntity()).isEqualTo(pkEntity);
        assertThat(record.getFkClass()).isEqualTo(classId);
        assertThat(record.getTypeId()).isEqualTo(entityTypeId);
        assertThat(record.getFkType()).isEqualTo(fkType);
        assertThat(record.getEntityLabel()).isEqualTo(entityLabel);
        assertThat(record.getClassLabel()).isEqualTo(entityClassLabel);
        assertThat(record.getFullText()).isEqualTo("Fulltext2");
        assertThat(record.getTimeSpan()).isEqualTo(entityTimeSpanJson);
        assertThat(record.getFirstSecond()).isEqualTo(entityFirstSecond);
        assertThat(record.getLastSecond()).isEqualTo(entityLastSecond);
        assertThat(record.getParentClasses()).isEqualTo(parentClassesJson);
        assertThat(record.getAncestorClasses()).isEqualTo(ancestorClassesJson);
        assertThat(record.getEntityType()).isEqualTo("teEn");

    }

}
