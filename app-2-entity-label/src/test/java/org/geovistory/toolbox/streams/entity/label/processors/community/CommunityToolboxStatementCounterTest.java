package org.geovistory.toolbox.streams.entity.label.processors.community;


import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label.processsors.community.CommunityToolboxStatementCounter;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CommunityToolboxStatementCounterTest {

    private static final String SCHEMA_REGISTRY_SCOPE = CommunityToolboxStatementCounterTest.class.getName();
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
        KeyValueStore<CommunityStatementKey, DRMap> store =
                Stores.keyValueStoreBuilder(
                                Stores.inMemoryKeyValueStore(storeName),
                                avroSerdes.CommunityStatementKey(),
                                avroSerdes.DRMap())
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

        var transformer = new CommunityToolboxStatementCounter(storeName);
        transformer.init(processorContext);

        var key1 = ProjectStatementKey.newBuilder()
                .setStatementId(1).setProjectId(1).build();
        var value1 = ProjectStatementValue.newBuilder()
                .setStatementId(1)
                .setProjectId(1)
                .setStatement(
                        StatementEnrichedValue.newBuilder().setSubjectId("i1").setPropertyId(2)
                                .setSubject(
                                        NodeValue.newBuilder().setLabel("Name 2").setId("i1").setClassId(0)
                                                .setEntity(
                                                        Entity.newBuilder()
                                                                .setFkClass(1)
                                                                .setCommunityVisibilityWebsite(false)
                                                                .setCommunityVisibilityDataApi(false)
                                                                .setCommunityVisibilityToolbox(true)
                                                                .build())
                                                .build()
                                )
                                .setObjectLabel("i2")
                                .setObject(NodeValue.newBuilder().setLabel("Name 2").setId("i2").setClassId(0)
                                        .setEntity(
                                                Entity.newBuilder()
                                                        .setFkClass(1)
                                                        .setCommunityVisibilityWebsite(false)
                                                        .setCommunityVisibilityDataApi(false)
                                                        .setCommunityVisibilityToolbox(true)
                                                        .build())
                                        .build()).build()
                )
                .setOrdNumOfDomain(1)
                .setOrdNumOfRange(2)
                .setDeleted$1(false)
                .build();

        // add to project 1
        var res = transformer.transform(key1, value1).value;
        assertThat(res.getProjectCount()).isEqualTo(1);
        assertThat(res.getAvgOrdNumOfDomain()).isEqualTo(1);
        assertThat(res.getAvgOrdNumOfRange()).isEqualTo(2);


        // add to project 2
        value1.setProjectId(2);
        value1.setOrdNumOfDomain(null);
        value1.setOrdNumOfRange(1);
        res = transformer.transform(key1, value1).value;
        assertThat(res.getProjectCount()).isEqualTo(2);
        assertThat(res.getAvgOrdNumOfDomain()).isEqualTo(1);
        assertThat(res.getAvgOrdNumOfRange()).isEqualTo(1.5f);

        // remove from project 2
        value1.setDeleted$1(true);
        res = transformer.transform(key1, value1).value;
        assertThat(res.getProjectCount()).isEqualTo(1);
        assertThat(res.getAvgOrdNumOfDomain()).isEqualTo(1);
        assertThat(res.getAvgOrdNumOfRange()).isEqualTo(2);


        // add to project 3
        value1.setProjectId(3);
        value1.setDeleted$1(false);
        value1.getStatement().getObject().getEntity().setCommunityVisibilityToolbox(false);
        res = transformer.transform(key1, value1).value;
        assertThat(res.getProjectCount()).isEqualTo(1);
        assertThat(res.getAvgOrdNumOfDomain()).isEqualTo(1);
        assertThat(res.getAvgOrdNumOfRange()).isEqualTo(2);

        // add to project 3
        value1.getStatement().getObject().getEntity().setCommunityVisibilityToolbox(true);
        value1.setOrdNumOfDomain(4);
        value1.setOrdNumOfRange(1);
        res = transformer.transform(key1, value1).value;
        assertThat(res.getProjectCount()).isEqualTo(2);
        assertThat(res.getAvgOrdNumOfDomain()).isEqualTo(5 / 2f);
        assertThat(res.getAvgOrdNumOfRange()).isEqualTo(3 / 2f);

    }


}
