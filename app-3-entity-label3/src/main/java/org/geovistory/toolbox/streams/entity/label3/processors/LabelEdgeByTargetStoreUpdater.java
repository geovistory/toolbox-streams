package org.geovistory.toolbox.streams.entity.label3.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.entity.label3.stores.LabelEdgeByTargetStore;
import ts.projects.entity_label_config.Key;
import ts.projects.entity_label_config.Value;

// Processor that keeps the global store updated.
public class LabelEdgeByTargetStoreUpdater implements Processor<Key, Value, Key, Value> {

    private KeyValueStore<Key, Value> store;

    private ProcessorContext<Key, Value> context;

    @Override
    public void init(final ProcessorContext<Key, Value> context) {
        store = context.getStateStore(LabelEdgeByTargetStore.NAME);
        this.context = context;
    }

    @Override
    public void process(final Record<Key, Value> record) {
        store.put(record.key(), record.value());
    }

    @Override
    public void close() {
        // No-op
    }

}