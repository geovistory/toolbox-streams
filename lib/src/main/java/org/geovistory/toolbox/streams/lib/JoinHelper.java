package org.geovistory.toolbox.streams.lib;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;
import java.util.Map;

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

        // create a state store builder
        Map<String, String> changelogConfig = new HashMap<>();
        StoreBuilder<KeyValueStore<InKey, InVal>> storeSupplier =
                Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(stateStoreName),
                                keySerdes,
                                valueSerdes)
                        .withLoggingEnabled(changelogConfig);

        // add state store
        topology.addStateStore(
                storeSupplier,
                inputName + "-process",
                additionalStateStoreProcessName
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


}
