package org.geovistory.toolbox.streams.entity.label3.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.LabelEdge;
import org.geovistory.toolbox.streams.entity.label3.lib.Fn;
import org.geovistory.toolbox.streams.entity.label3.stores.LabelEdgeBySourceStore;

// Processor that keeps the label edges by source store updated.
public class LabelEdgeBySourceStoreUpdater implements Processor<String, LabelEdge, String, LabelEdge> {

    private KeyValueStore<String, LabelEdge> store;

    private ProcessorContext<String, LabelEdge> context;

    @Override
    public void init(final ProcessorContext<String, LabelEdge> context) {
        store = context.getStateStore(LabelEdgeBySourceStore.NAME);
        this.context = context;
    }

    @Override
    public void process(final Record<String, LabelEdge> record) {
        if (record.value() == null) return;
        var newK = Fn.createLabelEdgeSourceKey(record.value());
        // if deleted, delete from store
        if (record.value().getDeleted()) store.delete(newK);
            // else put in store
        else store.put(newK, record.value());
        context.forward(record.withKey(newK));
    }

    @Override
    public void close() {
        // No-op
    }

}