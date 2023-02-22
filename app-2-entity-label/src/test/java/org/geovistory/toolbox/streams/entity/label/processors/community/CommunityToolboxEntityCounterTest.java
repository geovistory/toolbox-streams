package org.geovistory.toolbox.streams.entity.label.processors.community;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.geovistory.toolbox.streams.avro.CommunityEntityKey;
import org.geovistory.toolbox.streams.avro.CommunityEntityValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityVisibilityValue;
import org.geovistory.toolbox.streams.entity.label.processsors.community.CommunityToolboxEntity;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CommunityToolboxEntityCounterTest {

    private static final String SCHEMA_REGISTRY_SCOPE = CommunityToolboxEntityCounterTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    MockProcessorContext processorContext;

    String storeName = "myStore";

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        AppConfig.INSTANCE.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);
        processorContext = new MockProcessorContext(props);
        var avroSerdes = new ConfluentAvroSerdes();
        KeyValueStore<CommunityEntityKey, CommunityEntityValue> store =
                Stores.keyValueStoreBuilder(
                                Stores.inMemoryKeyValueStore(storeName),
                                avroSerdes.CommunityEntityKey(),
                                avroSerdes.CommunityEntityValue())
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

        var transformer = new CommunityToolboxEntity.Counter(storeName);
        transformer.init(processorContext);

        var key1 = ProjectEntityKey.newBuilder()
                .setEntityId("i1").setProjectId(1).build();
        var value1 = ProjectEntityVisibilityValue.newBuilder()
                .setEntityId("i1")
                .setProjectId(1)
                .setClassId(1)
                .setCommunityVisibilityToolbox(true)
                .setCommunityVisibilityDataApi(true)
                .setCommunityVisibilityWebsite(true)
                .setDeleted$1(false)
                .build();

        // add to project 1
        assertThat(transformer.transform(key1, value1).value.getProjectCount()).isEqualTo(1);

        // add to project 2
        value1.setProjectId(2);
        assertThat(transformer.transform(key1, value1).value.getProjectCount()).isEqualTo(2);

        // passing the same key-value pair again, should not change the count
        assertThat(transformer.transform(key1, value1).value.getProjectCount()).isEqualTo(2);

        // add change toolbox visibility
        value1.setCommunityVisibilityToolbox(false);
        assertThat(transformer.transform(key1, value1).value.getProjectCount()).isEqualTo(1);

        // set deleted to true, should reduce count by 1
        value1.setDeleted$1(true);
        assertThat(transformer.transform(key1, value1).value.getProjectCount()).isEqualTo(1);

        // passing the same key-value pair again, should not change the count
        assertThat(transformer.transform(key1, value1).value.getProjectCount()).isEqualTo(1);


    }


}
