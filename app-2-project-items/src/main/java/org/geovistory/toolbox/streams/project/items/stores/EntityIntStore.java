package org.geovistory.toolbox.streams.project.items.stores;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;

/**
 * Store to persist the number of projects of an entity and the class id of an entity
 * for the number of projects of an entity
 * key: {entityId}_n
 * for the class id of an entity
 * key: {entityId}_c
 */
@ApplicationScoped
public class EntityIntStore extends AbstractStore<String, Integer> {
    public static final String NAME = "entity-count-store";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Serde<String> getKeySerde() {
        return Serdes.String();
    }

    @Override
    public Serde<Integer> getValueSerde() {
        return Serdes.Integer();
    }

    public static String createNumberKey(String entityId) {
        return entityId + "_n";
    }

    public static String createClassKey(String entityId) {
        return entityId + "_c";
    }
}
