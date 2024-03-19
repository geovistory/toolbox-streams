package org.geovistory.toolbox.streams.entity.label3.processors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.EntityLabel;
import org.geovistory.toolbox.streams.avro.LabelEdge;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.entity.label3.stores.EntityLabelStore;
import org.geovistory.toolbox.streams.entity.label3.stores.LabelEdgeByTargetStore;

public class LabelWithEdgeJoiner implements Processor<ProjectEntityKey, EntityLabel, String, LabelEdge> {

    private KeyValueStore<String, LabelEdge> edgeStore;
    private KeyValueStore<ProjectEntityKey, EntityLabel> labelStore;
    private ProcessorContext<String, LabelEdge> context;

    @Override
    public void init(final ProcessorContext<String, LabelEdge> context) {
        edgeStore = context.getStateStore(LabelEdgeByTargetStore.NAME);
        labelStore = context.getStateStore(EntityLabelStore.NAME);
        this.context = context;
    }

    @Override
    public void process(final Record<ProjectEntityKey, EntityLabel> record) {
        var eId = record.key().getEntityId();
        var pId = record.key().getProjectId();

        // update store
        labelStore.put(record.key(), record.value());

        // prefix scan entityId_projectId_ and iterate
        try (var iterator = edgeStore.prefixScan(eId + "_" + pId + "_", Serdes.String().serializer())) {
            while (iterator.hasNext()) {
                // for each item
                var item = iterator.next();
                var edgeOld = item.value;
                if (edgeOld != null) {
                    // get old label
                    var labelOld = new EntityLabel(edgeOld.getTargetLabel(), edgeOld.getTargetLabelLanguage());
                    var labelNew = record.value();
                    // if new label differs from old...
                    if (!labelOld.equals(labelNew)) {
                        // create new join val
                        edgeOld.setTargetLabel(labelNew.getLabel());
                        edgeOld.setTargetLabelLanguage(labelNew.getLanguage());
                        var edgeNew = edgeOld;
                        // ...update store
                        edgeStore.put(item.key, edgeNew);
                        // ...push downstream
                        context.forward(record.withKey(item.key).withValue(edgeNew));
                    }
                }
            }
        }

    }

    @Override
    public void close() {
        // No-op
    }

}