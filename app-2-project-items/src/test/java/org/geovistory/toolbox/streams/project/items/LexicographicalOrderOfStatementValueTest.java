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
import org.geovistory.toolbox.streams.avro.StatementValue;
import org.geovistory.toolbox.streams.project.items.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.project.items.lib.Fn;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LexicographicalOrderOfStatementValueTest {

    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_STORE = "output-store-2";

    private static TopologyTestDriver testDriver;
    private static TestInputTopic<Integer, StatementValue> inputTopic;


    @BeforeAll
    public static void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "lexicographical-order-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        var as = new ConfiguredAvroSerde();
        as.schemaRegistryUrl = "mock://" + LexicographicalOrderOfStatementValueTest.class.getName();

        Topology topology = new Topology()
                .addSource("Source", Serdes.Integer().deserializer(), as.<StatementValue>value().deserializer(), INPUT_TOPIC)
                .addProcessor("Processor", StringConversionProcessor::new, "Source")
                .addStateStore(Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(OUTPUT_STORE),
                                Serdes.String(),
                                Serdes.Integer()),
                        "Processor");

        testDriver = new TopologyTestDriver(topology, config);

        inputTopic = testDriver.createInputTopic(INPUT_TOPIC, Serdes.Integer().serializer(), as.<StatementValue>value().serializer());
    }

    @AfterAll
    public static void tearDown() {
        testDriver.close();
    }

    @Test
    public void testLexicographicalOrderOfStatement() {
        // Publish test inputs
        pipeStatementValue(1, 5, 6, "i1", 1, "i2", 0f, null, "2022-07-21T12:26:00.643727Z");
        pipeStatementValue(2, 5, 6, "i1", 1, "i2", 0f, null, "2021-07-21T12:26:00.643727Z");
        pipeStatementValue(3, 5, 6, "i1", 1, "i2", 0f, null, "2023-07-21T12:26:00.643727Z");
        pipeStatementValue(4, 5, 6, "i1", 1, "i2", 0f, 0f, "2020-07-21T12:26:00.643727Z");
        pipeStatementValue(5, 5, 6, "i1", 1, "i2", 0f, 2f, "2021-07-21T12:26:00.643727Z");
        pipeStatementValue(6, 5, 6, "i1", 1, "i2", 0f, 1.4f, "2022-07-21T12:26:00.643727Z");
        pipeStatementValue(7, 5, 6, "i1", 1, "i2", 0f, 2.342f, "2023-07-21T12:26:00.643727Z");
        pipeStatementValue(8, 5, 6, "i1", 1, "i2", 0f, 1f, "2024-07-21T12:26:00.643727Z");

        // Retrieve stored strings from the persistent state store
        ReadOnlyKeyValueStore<String, Integer> store = testDriver.getKeyValueStore(OUTPUT_STORE);

        // scan by {class}_{project}_{entity}_{property}_
        KeyValueIterator<String, Integer> iterator = store.prefixScan("6_5_i1_1_", Serdes.String().serializer());

        // Verify lexicographical ordering
        List<Integer> retrievedStrings = new ArrayList<>();
        while (iterator.hasNext()) {
            retrievedStrings.add(iterator.next().value);
        }
        iterator.close();

        // Expected lexicographical order based on the input numbers
        List<Integer> expectedOrder = List.of(4, 8, 6, 5, 7, 2, 1, 3);

        assertEquals(expectedOrder, retrievedStrings);
    }

    private static void pipeStatementValue(int statementId, int projectId, int subjectClassId, String subjectId, int propertyId, String objectId, Float ordNumOfDomain, Float ordNumOfRange, String modifiedAt) {
        inputTopic.pipeInput(statementId, StatementValue.newBuilder()
                .setStatementId(statementId)
                .setProjectId(projectId)
                .setSubjectClassId(subjectClassId)
                .setSubjectId(subjectId)
                .setPropertyId(propertyId)
                .setObjectId(objectId)
                .setOrdNumOfDomain(ordNumOfDomain)
                .setOrdNumOfRange(ordNumOfRange)
                .setModifiedAt(modifiedAt).build()
        );
    }

    private static class StringConversionProcessor implements Processor<Integer, StatementValue, String, Integer> {


        private KeyValueStore<String, Integer> store;

        @Override
        public void init(ProcessorContext context) {
            this.store = context.getStateStore(OUTPUT_STORE);
        }

        @Override
        public void process(Record<Integer, StatementValue> record) {
            var val = record.value();
            var key = createSortableStatementKeyBySubject(val);
            store.put(key, record.key()); // Store in the persistent state store
        }

        /**
         * Creates a statement key  based on the provided parameters in this form:
         * {subjectClass}_{project}_{subject}_{property}_{ordNumOfRange}_{modifiedAt}
         *
         * @param val The StatementValue object containing project ID, property ID, and ordinal number of range.
         * @return A string representing the statement key.
         */
        public static String createSortableStatementKeyBySubject(StatementValue val) {

            var entity = val.getSubjectId();
            var classId = val.getSubjectClassId();
            var ordNum = val.getOrdNumOfRange();
            return createSortableStatementKey(val, entity, classId, ordNum);

        }

        /**
         * Creates a statement key based on the provided parameters in this form:
         * {class}_{project}_{entity}_{property}_{ordNum}_{modifiedAt}
         *
         * @param val     The StatementValue object containing project ID, property ID, and ordinal number of range.
         * @param entity  The entity associated with the statement.
         * @param classId The class ID associated with the statement.
         * @param ordNum  The ordinal number of range associated with the statement.
         * @return A string representing the statement key.
         */
        private static String createSortableStatementKey(StatementValue val, String entity, Integer classId, Float ordNum) {
            var project = val.getProjectId();
            var property = val.getPropertyId();
            var modifiedAt = val.getModifiedAt();
            String ordNumStr = (ordNum == null) ? "zzzzzzzz" : Fn.floatToHexString(val.getOrdNumOfRange());
            String modifiedAtStr = modifiedAt == null ? "Z" : modifiedAt;
            return classId + "_" + project + "_" + entity + "_" + property + "_" + ordNumStr + "_" + modifiedAtStr;
        }

        @Override
        public void close() {
            // Close resources if needed
        }
    }
}
