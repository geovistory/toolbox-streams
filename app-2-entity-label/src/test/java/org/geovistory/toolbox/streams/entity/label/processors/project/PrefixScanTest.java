package org.geovistory.toolbox.streams.entity.label.processors.project;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Properties;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class PrefixScanTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Long> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;
    private KeyValueStore<String, Long> store;

    private final Serde<String> stringSerde = new Serdes.StringSerde();
    private final Serde<Long> longSerde = new Serdes.LongSerde();

    @BeforeEach
    public void setup() {
        final Topology topology = new Topology();
        topology.addSource("sourceProcessor", "input-topic");
        topology.addProcessor("aggregator", new CustomMaxAggregatorSupplier(), "sourceProcessor");

        topology.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("aggStore"),
                        Serdes.String(),
                        Serdes.Long()).withLoggingDisabled(), // need to disable logging to allow store pre-populating
                "aggregator");

        // setup test driver
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "maxAggregation");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        testDriver = new TopologyTestDriver(topology, props);

        // setup test topics
        inputTopic = testDriver.createInputTopic("input-topic", stringSerde.serializer(), longSerde.serializer());
        outputTopic = testDriver.createOutputTopic("result-topic", stringSerde.deserializer(), longSerde.deserializer());

        // pre-populate store
        store = testDriver.getKeyValueStore("aggStore");
        store.put("a-b-123", 1L);
        store.put("a-c-345", 2L);
    }


    @Test
    void testPrefixScan() {

        var iterator = store.prefixScan("a", new StringSerializer());

        var result = new ArrayList<Long>();
        while (iterator.hasNext()) {
            var item = iterator.next();
            result.add(item.value);
        }

        assertThat(result.get(0)).isEqualTo(1L);
        assertThat(result.get(1)).isEqualTo(2L);

    }

    @Test
    void testPrefixScan2() {

        var iterator = store.prefixScan("a-c", new StringSerializer());

        var result = new ArrayList<Long>();
        while (iterator.hasNext()) {
            var item = iterator.next();
            result.add(item.value);
        }

        assertThat(result.get(0)).isEqualTo(2L);

    }


    @Test
    void testDeleteScan2() {

        var iterator = store.prefixScan("a", new StringSerializer());

        while (iterator.hasNext()) {
            var item = iterator.next();
            store.delete(item.key);
        }

        assertThat(store.all().hasNext()).isEqualTo(false);

    }


    public static class CustomMaxAggregatorSupplier implements ProcessorSupplier<String, Long, String, Long> {
        @Override
        public Processor<String, Long, String, Long> get() {
            return new CustomMaxAggregator();
        }
    }

    public static class CustomMaxAggregator implements Processor<String, Long, String, Long> {
        ProcessorContext context;
        private KeyValueStore<String, Long> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            this.context = context;
            store = context.getStateStore("aggStore");
        }

        @Override
        public void process(Record record) {

        }


        @Override
        public void close() {
        }
    }

}
