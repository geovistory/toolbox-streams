package org.geovistory.toolbox.streams.entity.label3.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

// Processor that keeps the global store updated.
public class GlobalStoreUpdater<K, V> implements Processor<K, V, Void, Void> {

    private final String storeName;


    public GlobalStoreUpdater(final String storeName) {
        this.storeName = storeName;
    }

    private KeyValueStore<K, V> store;

    @Override
    public void init(final ProcessorContext<Void, Void> processorContext) {
        store = processorContext.getStateStore(storeName);
    }

    @Override
    public void process(final Record<K, V> record) {
        // We are only supposed to put operation the keep the store updated.
        // We should not filter record or modify the key or value
        // Doing so would break fault-tolerance.
        // see https://issues.apache.org/jira/browse/KAFKA-7663
        store.put(record.key(), record.value());
    }

    @Override
    public void close() {
        // No-op
    }

}