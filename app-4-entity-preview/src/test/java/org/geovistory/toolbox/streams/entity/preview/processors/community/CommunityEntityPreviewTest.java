package org.geovistory.toolbox.streams.entity.preview.processors.community;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.preview.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CommunityEntityPreviewTest {

    private static final String SCHEMA_REGISTRY_SCOPE = CommunityEntityPreviewTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver testDriver;
    private TestInputTopic<CommunityEntityKey, CommunityEntityValue> communityEntityTopic;
    private TestInputTopic<CommunityEntityKey, CommunityEntityLabelValue> communityEntityLabelTopic;
    private TestInputTopic<CommunityEntityKey, CommunityEntityClassLabelValue> communityEntityClassLabelTopic;
    private TestInputTopic<CommunityEntityKey, CommunityEntityTypeValue> communityEntityTypeTopic;
    private TestInputTopic<CommunityEntityKey, TimeSpanValue> communityEntityTimeSpanTopic;
    private TestInputTopic<CommunityEntityKey, CommunityEntityFulltextValue> communityEntityFulltextTopic;
    private TestInputTopic<CommunityEntityKey, CommunityEntityClassMetadataValue> communityEntityClassMetadataTopic;
    private TestOutputTopic<CommunityEntityKey, EntityPreviewValue> outputTopic;

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
        var communityClassLabel = new CommunityEntityPreview(avroSerdes, registerInputTopic, outputTopicNames);
        communityClassLabel.addProcessorsStandalone();
        var topology = builderSingleton.builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        communityEntityTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityEntity(),
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.CommunityEntityValue().serializer());

        communityEntityLabelTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityEntityLabel(),
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.CommunityEntityLabelValue().serializer());

        communityEntityClassLabelTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityEntityClassLabel(),
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.CommunityEntityClassLabelValue().serializer());

        communityEntityTypeTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityEntityType(),
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.CommunityEntityTypeValue().serializer());

        communityEntityTimeSpanTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityEntityTimeSpan(),
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.TimeSpanValue().serializer());

        communityEntityFulltextTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityEntityFulltext(),
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.CommunityEntityFulltextValue().serializer());

        communityEntityClassMetadataTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityEntityClassMetadata(),
                avroSerdes.CommunityEntityKey().serializer(),
                avroSerdes.CommunityEntityClassMetadataValue().serializer());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.communityEntityPreview(),
                avroSerdes.CommunityEntityKey().deserializer(),
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
        var kE = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        var vE = CommunityEntityValue.newBuilder().setEntityId(entityId).setClassId(classId).setProjectCount(1).build();
        communityEntityTopic.pipeInput(kE, vE);

        // add an entity label
        var vEL = CommunityEntityLabelValue.newBuilder().setEntityId(entityId).setLabel(entityLabel).setLabelSlots(List.of(entityLabel)).build();
        communityEntityLabelTopic.pipeInput(kE, vEL);

        // add an entity class label
        var vECL = CommunityEntityClassLabelValue.newBuilder().setClassLabel(entityClassLabel).build();
        communityEntityClassLabelTopic.pipeInput(kE, vECL);

        // add an entity type
        var vET = CommunityEntityTypeValue.newBuilder().setEntityId(entityId)
                .setTypeId(entityTypeId).setTypeLabel(entityTypeLabel).build();
        communityEntityTypeTopic.pipeInput(kE, vET);

        // add an entity time span
        var vETSP = TimeSpanValue.newBuilder().setTimeSpan(entityTimeSpan).setFirstSecond(entityFirstSecond).setLastSecond(entityLastSecond).build();
        communityEntityTimeSpanTopic.pipeInput(kE, vETSP);

        // add an entity fulltext
        var vEFT = CommunityEntityFulltextValue.newBuilder().setEntityId(entityId).setFulltext(entityFulltext).build();
        communityEntityFulltextTopic.pipeInput(kE, vEFT);

        // add an entity class metadata
        var vECM = CommunityEntityClassMetadataValue.newBuilder().setParentClasses(parentClasses).setAncestorClasses(ancestorClasses).build();
        communityEntityClassMetadataTopic.pipeInput(kE, vECM);
        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var record = outRecords.get(kE);
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
        var classId = 3;
        var parentClasses = List.of(1, 70, 3);
        var ancestorClasses = List.of(4, 5, 6);
        var parentClassesJson = "[1, 70, 3]";
        var ancestorClassesJson = "[4, 5, 6]";

        // add an entity
        var kE = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        var vE = CommunityEntityValue.newBuilder().setEntityId(entityId).setClassId(classId).setProjectCount(1).build();
        communityEntityTopic.pipeInput(kE, vE);


        // add an entity class metadata
        var vECM = CommunityEntityClassMetadataValue.newBuilder().setParentClasses(parentClasses).setAncestorClasses(ancestorClasses).build();
        communityEntityClassMetadataTopic.pipeInput(kE, vECM);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var record = outRecords.get(kE);
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
        var classId = 3;

        // add an entity
        var kE = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        var vE = CommunityEntityValue.newBuilder().setEntityId(entityId).setClassId(classId).setProjectCount(1).build();
        communityEntityTopic.pipeInput(kE, vE);

        // add an entity type
        var vET = CommunityEntityTypeValue.newBuilder().setEntityId(entityId)
                .setTypeId("").setTypeLabel("").build();
        communityEntityTypeTopic.pipeInput(kE, vET);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var record = outRecords.get(kE);
        assertThat(record.getEntityId()).isEqualTo(entityId);
        assertThat(record.getTypeId()).isEqualTo(null);
        assertThat(record.getFkType()).isEqualTo(null);


    }

    @Test
    void testDeleteEntityPreview() {

        var entityId = "i1";
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
        var kE = CommunityEntityKey.newBuilder().setEntityId(entityId).build();
        var vE = CommunityEntityValue.newBuilder().setEntityId(entityId).setClassId(classId).setProjectCount(1).build();
        communityEntityTopic.pipeInput(kE, vE);

        // add an entity label
        var vEL = CommunityEntityLabelValue.newBuilder().setEntityId(entityId).setLabel(entityLabel).setLabelSlots(List.of(entityLabel)).build();
        communityEntityLabelTopic.pipeInput(kE, vEL);

        // add an entity class label
        var vECL = CommunityEntityClassLabelValue.newBuilder().setClassLabel(entityClassLabel).build();
        communityEntityClassLabelTopic.pipeInput(kE, vECL);

        // add an entity type
        var vET = CommunityEntityTypeValue.newBuilder().setEntityId(entityId)
                .setTypeId(entityTypeId).setTypeLabel(entityTypeLabel).build();
        communityEntityTypeTopic.pipeInput(kE, vET);

        // add an entity time span
        var vETSP = TimeSpanValue.newBuilder().setTimeSpan(entityTimeSpan).setFirstSecond(entityFirstSecond).setLastSecond(entityLastSecond).build();
        communityEntityTimeSpanTopic.pipeInput(kE, vETSP);

        // add an entity fulltext
        var vEFT = CommunityEntityFulltextValue.newBuilder().setEntityId(entityId).setFulltext(entityFulltext).build();
        communityEntityFulltextTopic.pipeInput(kE, vEFT);

        // add an entity class metadata
        var vECM = CommunityEntityClassMetadataValue.newBuilder().setParentClasses(parentClasses).setAncestorClasses(ancestorClasses).build();
        communityEntityClassMetadataTopic.pipeInput(kE, vECM);

        // delete entity
        vE.setProjectCount(0);
        communityEntityTopic.pipeInput(kE, vE);

        assertThat(outputTopic.isEmpty()).isFalse();
        var outRecords = outputTopic.readKeyValuesToMap();
        assertThat(outRecords).hasSize(1);

        var record = outRecords.get(kE);
        assertThat(record).isNull();
    }


}
