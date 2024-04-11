package org.geovistory.toolbox.streams.entity.stores;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;

/**
 * Stores the sortable key of a time-primitives
 * with key: {sourceId}_{projectId}_{propertyId}_{isOutgoing}_{targetId}
 * with val: {sourceId}_{projectId}_{propertyId}_{isOutgoing}_{ordNum}_{modifiedAt}_{targetId}
 */
@ApplicationScoped
public class TimePrimitiveSortKeyStore extends AbstractStore<String, String> {
    public static final String NAME = "time-primitive-sort-key-store";

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
