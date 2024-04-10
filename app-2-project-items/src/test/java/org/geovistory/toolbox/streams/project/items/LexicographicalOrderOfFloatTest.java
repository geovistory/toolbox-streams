package org.geovistory.toolbox.streams.project.items;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.geovistory.toolbox.streams.project.items.lib.Fn;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LexicographicalOrderOfFloatTest {

    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_STORE = "output-store";

    private static TopologyTestDriver testDriver;
    private static TestInputTopic<Float, String> inputTopic;


    @BeforeAll
    public static void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "lexicographical-order-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Topology topology = new Topology()
                .addSource("Source", Serdes.Float().deserializer(), Serdes.String().deserializer(), INPUT_TOPIC)
                .addProcessor("Processor", StringConversionProcessor::new, "Source")
                .addStateStore(Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(OUTPUT_STORE),
                                Serdes.String(),
                                Serdes.String()),
                        "Processor");

        testDriver = new TopologyTestDriver(topology, config);
        inputTopic = testDriver.createInputTopic(INPUT_TOPIC, Serdes.Float().serializer(), Serdes.String().serializer());
    }

    @AfterAll
    public static void tearDown() {
        testDriver.close();
    }

    @Test
    public void testLexicographicalOrder() {
        // Publish test input numbers
        inputTopic.pipeInput(3.14f, "3.14f");
        inputTopic.pipeInput(1.618f, "1.618f");
        inputTopic.pipeInput(2.718f, "2.718f");
        inputTopic.pipeInput(2f, "2f");
        inputTopic.pipeInput(0.2f, "0.2f");
        inputTopic.pipeInput(0.12312f, "0.12312f");
        inputTopic.pipeInput(100f, "100f");
        inputTopic.pipeInput(999999999f, "999999999f");

        // Retrieve stored strings from the persistent state store
        ReadOnlyKeyValueStore<String, String> store = testDriver.getKeyValueStore(OUTPUT_STORE);
        KeyValueIterator<String, String> iterator = store.prefixScan("my_prefix_", Serdes.String().serializer());

        // Verify lexicographical ordering
        List<String> retrievedStrings = new ArrayList<>();
        while (iterator.hasNext()) {
            retrievedStrings.add(iterator.next().value);
        }
        iterator.close();

        // Expected lexicographical order based on the input numbers
        List<String> expectedOrder = List.of("0.12312f", "0.2f", "1.618f", "2f", "2.718f", "3.14f", "100f", "999999999f");

        assertEquals(expectedOrder, retrievedStrings);
    }

    private static class StringConversionProcessor implements Processor<Float, String, String, String> {


        private KeyValueStore<String, String> store;

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.store = context.getStateStore(OUTPUT_STORE);
        }

        @Override
        public void process(Record<Float, String> record) {
            String convertedString = Fn.floatToHexString(record.key());
            store.put("my_prefix_" + convertedString, record.value()); // Store in the persistent state store
        }

        @Override
        public void close() {
            // Close resources if needed
        }
    }
}
