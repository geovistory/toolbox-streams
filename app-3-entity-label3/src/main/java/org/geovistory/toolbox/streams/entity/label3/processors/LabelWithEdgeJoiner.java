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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class LabelWithEdgeJoiner implements Processor<ProjectEntityKey, EntityLabel, String, LabelEdge> {
    private static final Logger LOG = LoggerFactory.getLogger(LabelWithEdgeJoiner.class);
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
    public void process(final Record<ProjectEntityKey, EntityLabel> record) {
        LOG.info("process() called with record: {}", record);
        var eId = record.key().getEntityId();
        var pId = record.key().getProjectId();

        // update store
        labelStore.put(record.key(), record.value());

        // prefix scan entityId_projectId_ and iterate
        try (var iterator = edgeByTargetStore.prefixScan(eId + "_" + pId + "_", Serdes.String().serializer())) {
            while (iterator.hasNext()) {
                // for each item
                var item = iterator.next();
                var edge = item.value;
                if (edge != null) {
                    // get old label
                    EntityLabel oldLabel = null;
                    if (edge.getTargetLabel() != null && edge.getTargetLabelLanguage() != null) {
                        oldLabel = EntityLabel.newBuilder()
                                .setLabel(edge.getTargetLabel())
                                .setLanguage(edge.getTargetLabelLanguage()).build();
                    }
                    EntityLabel newLabel = record.value();

                    // if new label differs from old...
                    if (!Objects.equals(oldLabel, newLabel)) {

                        // if new label exists
                        if (newLabel != null) {
                            // ... join the label
                            edge.setTargetLabel(newLabel.getLabel());
                            edge.setTargetLabelLanguage(newLabel.getLanguage());
                        } else {
                            // ... mark edge as deleted
                            edge.setDeleted(true);
                        }
                        // ... update store
                        edgeByTargetStore.put(item.key, edge);

                        // ...push downstream
                        context.forward(record.withKey(item.key).withValue(edge));
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