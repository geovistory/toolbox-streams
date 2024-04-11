package org.geovistory.toolbox.streams.entity.preview.processors.community;


import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.kafka.streams.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.preview.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.preview.InputTopicNames;
import org.geovistory.toolbox.streams.entity.preview.OutputTopicNames;
import org.geovistory.toolbox.streams.testlib.TopologyTestDriverProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
@TestProfile(TopologyTestDriverProfile.class)
class CommunityEntityPreviewTest {

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


        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testApplicationId");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        testDriver = new TopologyTestDriver(topology, config);


        communityEntityTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityEntity(),
                as.kS(),
                as.vS());

        communityEntityLabelTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityEntityLabel(),
                as.kS(),
                as.vS());

        communityEntityClassLabelTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityEntityClassLabel(),
                as.kS(),
                as.vS());

        communityEntityTypeTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityEntityType(),
                as.kS(),
                as.vS());

        communityEntityTimeSpanTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityEntityTimeSpan(),
                as.kS(),
                as.vS());

        communityEntityFulltextTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityEntityFulltext(),
                as.kS(),
                as.vS());

        communityEntityClassMetadataTopic = testDriver.createInputTopic(
                inputTopicNames.getCommunityEntityClassMetadata(),
                as.kS(),
                as.vS());

        outputTopic = testDriver.createOutputTopic(
                outputTopicNames.communityEntityPreview(),
                as.kD(),
                as.vD());
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
