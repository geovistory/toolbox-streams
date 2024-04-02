package org.geovistory.toolbox.streams.entity.label3.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.avro.InProjectOrdNum;
import org.geovistory.toolbox.streams.avro.LabelEdge;
import org.geovistory.toolbox.streams.entity.label3.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;

/**
 * Store to keep record of edge-project relation and the ord num of an edge
 * with key: slug_sourceID_propertyID_targetID_projectID
 * where slug is public or toolbox
 * with val: ord_num
 * -1 means, this edge has no ord_num but
 * it is still adds to the project count
 */
@ApplicationScoped
public class EdgeOrdNumStore extends AbstractStore<String, InProjectOrdNum> {
    public static final String NAME = "edge-bool-store";
    @Inject
    ConfiguredAvroSerde as;

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Serde<String> getKeySerde() {
        return Serdes.String();
    }

    @Override
    public Serde<InProjectOrdNum> getValueSerde() {
        return as.value();
    }

    public static String createKey(LabelEdge e) {
        return String.join("_", new String[]{e.getSourceId(), e.getPropertyId() + "", e.getIsOutgoing() + "", e.getTargetId(), e.getProjectId() + ""});
    }

}
