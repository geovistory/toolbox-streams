package org.geovistory.toolbox.streams.lib;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.jboss.logging.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Helper class for the implementation of joins using
 * Kafka Streams Processor API
 */
public class JoinHelper {

    public static <InKey, InVal> void registerSourceTopics(Topology topology, String inputName, Serde<InKey> keySerdes, Serde<InVal> valueSerdes) {

        // add source
        topology.addSource(
                inputName + "-source",
                keySerdes.deserializer(),
                valueSerdes.deserializer(),
                inputName
        );

        // add input processor
        topology.addProcessor(
                inputName + "-process",
                () -> new InputProcessor<InKey, InVal>(inputName + "-store"),
                inputName + "-source"
        );


    }

    public static <InKey, InVal> void addStateStore(Topology topology, String inputName, Serde<InKey> keySerdes, Serde<InVal> valueSerdes, String additionalStateStoreProcessName) {
        var stateStoreName = inputName + "-store";
        var processorName = inputName + "-process";
        addStateStore(topology, stateStoreName, processorName, keySerdes, valueSerdes, additionalStateStoreProcessName);
    }

    public static <InKey, InVal> void addStateStore(Topology topology, String stateStoreName, String processorName, Serde<InKey> keySerdes, Serde<InVal> valueSerdes, String... additionalStateStoreProcessName) {

        // create a state store builder
        Map<String, String> changelogConfig = new HashMap<>();
        StoreBuilder<KeyValueStore<InKey, InVal>> storeSupplier =
                Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(stateStoreName),
                                keySerdes,
                                valueSerdes)
                        .withLoggingEnabled(changelogConfig);

        // add state store
        String[] processorNames = new String[additionalStateStoreProcessName.length + 1];
        processorNames[0] = processorName;
        System.arraycopy(additionalStateStoreProcessName, 0, processorNames, 1, additionalStateStoreProcessName.length);
        topology.addStateStore(
                storeSupplier,
                processorNames
        );

    }


    public static class InputProcessor<InKey, InVal> implements Processor<InKey, InVal, InKey, InVal> {
        String stateStoreName;
        private KeyValueStore<InKey, InVal> keyValueStore;
        private ProcessorContext<InKey, InVal> context;

        public InputProcessor(String stateStoreName) {
            this.stateStoreName = stateStoreName;
        }


        @Override
        public void process(Record<InKey, InVal> record) {
            // Put into state store.
            keyValueStore.put(record.key(), record.value());
            // Send downstream. Join processor just needs the key,
            // so don't need to send any value.
            context.forward(record.withValue(null));
        }

        @Override
        public void init(ProcessorContext<InKey, InVal> context) {
            this.keyValueStore = context.getStateStore(stateStoreName);
            this.context = context;
        }

        @Override
        public void close() {
        }
    }

    public static class InputProcessorWithStoreKey<InKey, InVal, StoreKey> implements Processor<InKey, InVal, InKey, InVal> {
        private static final Logger LOG = Logger.getLogger(InputProcessorWithStoreKey.class);

        String stateStoreName;
        private KeyValueStore<StoreKey, InVal> keyValueStore;
        private ProcessorContext<InKey, InVal> context;

        private final Function<Record<InKey, InVal>, StoreKey> mapStoreKey;

        public InputProcessorWithStoreKey(String stateStoreName, Function<Record<InKey, InVal>, StoreKey> mapStoreKey) {
            this.stateStoreName = stateStoreName;
            this.mapStoreKey = mapStoreKey;
        }

        @Override
        public void process(Record<InKey, InVal> record) {

            try {
                // Put into state store.
                keyValueStore.put(this.mapStoreKey.apply(record), record.value());
                // Send downstream. Join processor just needs the key,
                // so don't need to send any value.
                context.forward(record);
            } catch (Exception e) {
                LOG.warn(e.getMessage());
            }
        }

        @Override
        public void init(ProcessorContext<InKey, InVal> context) {
            this.keyValueStore = context.getStateStore(stateStoreName);
            this.context = context;
        }

        @Override
        public void close() {
        }
    }


}
