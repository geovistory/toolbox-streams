package org.geovistory.toolbox.streams.entity.label3.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.entity.label3.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;

/**
 * Stores the sortable key of a label edges
 * with key: {sourceId}_{projectId}_{propertyId}_{isOutgoing}_{targetId}
 * with val: {classId}_{sourceId}_{projectId}_{propertyId}_{isOutgoing}_{ordNum}_{modifiedAt}_{targetId}
 */
@ApplicationScoped
public class LabelEdgeSortKeyStore extends AbstractStore<String, String> {
    public static final String NAME = "label-edge-sort-key-store";
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
    public Serde<String> getValueSerde() {
        return Serdes.String();
    }

}
