package org.geovistory.toolbox.streams.entity.label3.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.EntityLabel;
import org.geovistory.toolbox.streams.avro.LabelEdge;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.entity.label3.stores.EntityLabelStore;
import org.geovistory.toolbox.streams.entity.label3.stores.LabelEdgeByTargetStore;

public class EdgeWithLabelJoiner implements Processor<String, LabelEdge, String, LabelEdge> {

    private KeyValueStore<String, LabelEdge> edgeByTargetStore;
    private KeyValueStore<ProjectEntityKey, EntityLabel> labelStore;

    private ProcessorContext<String, LabelEdge> context;

    @Override
    public void init(final ProcessorContext<String, LabelEdge> context) {
        edgeByTargetStore = context.getStateStore(LabelEdgeByTargetStore.NAME);
        labelStore = context.getStateStore(EntityLabelStore.NAME);
        this.context = context;
    }

    @Override
    public void process(final Record<String, LabelEdge> record) {
        if (record.value() == null) return;
        var edge = record.value();

        var eId = edge.getTargetId();
        var pId = edge.getProjectId();
        var pKey = eId + "_" + pId + "_" + record.key();
        var cKey = eId + "_0_" + pId + "_" + record.key();

        // lookup old join val entityId_projectId_{edge_key} (-> oP)
        var oP = edgeByTargetStore.get(pKey);
        // lookup old join val entityId_0_projectId_{edge_key} (-> oC)
        var oC = edgeByTargetStore.get(cKey);

        var oldVal = oP == null ? oC : null;

        EntityLabel newLabel;

        String futureStoreKey;

        // if edge has target project entity ...
        if (edge.getTargetIsInProject()) {
            // define key this edge will have in edgeStore
            futureStoreKey = pKey;
            // ... lookup project entity label
            newLabel = labelStore.get(new ProjectEntityKey(pId, eId));
            // ... if oC not null, delete it from join store
            if (oC != null) {
                edgeByTargetStore.delete(cKey);
                edgeByTargetStore.put(futureStoreKey, oC);
            }
        }

        // else ...
        else {
            // define key this edge will have in edgeStore
            futureStoreKey = cKey;
            // ... lookup community entity label
            newLabel = labelStore.get(new ProjectEntityKey(0, eId));
            // ... if oP not null, delete it from join store and add it with new key
            if (oP != null) {
                edgeByTargetStore.delete(pKey);
                edgeByTargetStore.put(futureStoreKey, oP);
            }
        }

        // we only want to push downstream, if
        // - the entity label is present and different from old join val
        // - the old val is present and entity label deleted
        // the other way around, we do not push downstream, if
        // - the entity label and the old join val are null
        // - the old join val and new join val are unchanged

        // if old==null and new==null ...
        if (oldVal == null && newLabel == null) {
            // ... add the un-joined value to the store
            edgeByTargetStore.put(futureStoreKey, edge);
            // stop here
            return;
        }

        // if new label exists
        if (newLabel != null) {
            edge.setTargetLabel(newLabel.getLabel());
            edge.setTargetLabelLanguage(newLabel.getLanguage());
        } else {
            // ... mark edge as deleted
            edge.setDeleted(true);
        }

        // if new value differs from old ...
        if (!edge.equals(oldVal)) {

            // ... update join store
            edgeByTargetStore.put(futureStoreKey, edge);

            // push downstream
            context.forward(record.withValue(edge));
        }
    }

    @Override
    public void close() {
        // No-op
    }

}