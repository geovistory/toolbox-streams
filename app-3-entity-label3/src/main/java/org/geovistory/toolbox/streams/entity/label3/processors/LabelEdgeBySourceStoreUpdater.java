package org.geovistory.toolbox.streams.entity.label3.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.LabelEdge;
import org.geovistory.toolbox.streams.entity.label3.lib.Fn;
import org.geovistory.toolbox.streams.entity.label3.stores.LabelEdgeBySourceStore;
import org.geovistory.toolbox.streams.entity.label3.stores.LabelEdgeSortKeyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.geovistory.toolbox.streams.lib.Utils.createEdgeUniqueKey;

// Processor that keeps the label edges by source store updated.
public class LabelEdgeBySourceStoreUpdater implements Processor<String, LabelEdge, String, LabelEdge> {
    private static final Logger LOG = LoggerFactory.getLogger(LabelEdgeBySourceStoreUpdater.class);

    private KeyValueStore<String, LabelEdge> sortedEdgeStore;
    private KeyValueStore<String, String> sortKeyStore;

    private ProcessorContext<String, LabelEdge> context;

    @Override
    public void init(final ProcessorContext<String, LabelEdge> context) {
        sortedEdgeStore = context.getStateStore(LabelEdgeBySourceStore.NAME);
        sortKeyStore = context.getStateStore(LabelEdgeSortKeyStore.NAME);

        this.context = context;
    }

    @Override
    public void process(final Record<String, LabelEdge> record) {
        LOG.debug("process() called with record: {}", record);
        if (record.value() == null) return;
        var edgeId = createEdgeUniqueKey(record.value());
        var sortKeyOld = sortKeyStore.get(edgeId);
        var sortKeyNew = Fn.createLabelEdgeSortableKey(record.value());
        var positionChanged = !Objects.equals(sortKeyOld, sortKeyNew);
        // if deleted or changed
        if ((record.value().getDeleted() || positionChanged) && sortKeyOld != null) {
            // delete edge from old position in sorted store
            sortedEdgeStore.delete(sortKeyOld);
        }
        // delete sort key for edge
        if (record.value().getDeleted()) sortKeyStore.delete(edgeId);

        if (!record.value().getDeleted()) {
            // put edge at new position in sorted store
            sortedEdgeStore.put(sortKeyNew, record.value());
            // update sort key for edge
            sortKeyStore.put(edgeId, sortKeyNew);
        }
        context.forward(record.withKey(sortKeyNew));
    }


    @Override
    public void close() {
        // No-op
    }

}