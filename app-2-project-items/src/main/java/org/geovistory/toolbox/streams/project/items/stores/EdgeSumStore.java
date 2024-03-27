package org.geovistory.toolbox.streams.project.items.stores;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.avro.EdgeValue;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;

/**
 * Store to sum ord-num of an edges
 * with key: slug_sourceID_propertyID_targetID_projectID
 * where slug is public or toolbox
 * with val: ord_num
 */
@ApplicationScoped
public class EdgeSumStore extends AbstractStore<String, Float> {
    public static final String NAME = "edge-sum-store";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Serde<String> getKeySerde() {
        return Serdes.String();
    }

    @Override
    public Serde<Float> getValueSerde() {
        return Serdes.Float();
    }

    public static String createKey(String slug, EdgeValue e) {
        return String.join("_", new String[]{slug, e.getSourceId(), e.getPropertyId() + "", e.getIsOutgoing() + "", e.getTargetId()});
    }
}
