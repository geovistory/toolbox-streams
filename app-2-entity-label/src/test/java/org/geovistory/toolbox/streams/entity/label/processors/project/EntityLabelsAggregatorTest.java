package org.geovistory.toolbox.streams.entity.label.processors.project;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.geovistory.toolbox.streams.avro.EntityLabelSlotWithStringValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityLabelValue;
import org.geovistory.toolbox.streams.entity.label.AvroSerdes;
import org.geovistory.toolbox.streams.entity.label.processsors.project.ProjectEntityLabel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class EntityLabelsAggregatorTest {

    private static final String SCHEMA_REGISTRY_SCOPE = EntityLabelsAggregatorTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    MockProcessorContext processorContext;

    String storeName = "myStore";

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        processorContext = new MockProcessorContext(props);
        var avroSerdes = new AvroSerdes();
        avroSerdes.QUARKUS_KAFKA_STREAMS_SCHEMA_REGISTRY_URL = MOCK_SCHEMA_REGISTRY_URL;
        KeyValueStore<ProjectEntityKey, ProjectEntityLabelValue> store =
                Stores.keyValueStoreBuilder(
                                Stores.inMemoryKeyValueStore(storeName),
                                avroSerdes.ProjectEntityKey(),
                                avroSerdes.ProjectEntityLabelValue())
                        .withLoggingDisabled()
                        .build();
        store.init(processorContext, store);
        processorContext.register(store, null);
    }

    @AfterEach
    void teardown() {

    }

    @Test
    public void testTransformer() {

        var transformer = new ProjectEntityLabel.EntityLabelsAggregator(storeName);
        transformer.init(processorContext);

        var key1 = ProjectEntityKey.newBuilder()
                .setEntityId("i1").setProjectId(1).build();
        var value1 = EntityLabelSlotWithStringValue.newBuilder()
                .setOrdNum(0).setString("A").setDeleted$1(false).build();

        assertThat(transformer.transform(key1, value1).value.getLabel()).isEqualTo("A");


        var key2 = ProjectEntityKey.newBuilder()
                .setEntityId("i1").setProjectId(1).build();
        var value2 = EntityLabelSlotWithStringValue.newBuilder()
                .setOrdNum(1).setString("B").setDeleted$1(false).build();

        assertThat(transformer.transform(key2, value2).value.getLabel()).isEqualTo("A, B");
    }


    @Test
    public void testTransformerWithNullValue() {

        var transformer = new ProjectEntityLabel.EntityLabelsAggregator(storeName);
        transformer.init(processorContext);


        var key2 = ProjectEntityKey.newBuilder()
                .setEntityId("i1").setProjectId(1).build();

        assertThat(transformer.transform(key2, null).value.getLabelSlots().size()).isEqualTo(ProjectEntityLabel.NUMBER_OF_SLOTS);
    }


}
