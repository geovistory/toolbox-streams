package org.geovistory.toolbox.streams.entity.label2.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.geovistory.toolbox.streams.avro.EntityValue;
import org.geovistory.toolbox.streams.entity.label2.lib.AbstractStore;
import org.geovistory.toolbox.streams.entity.label2.lib.ConfiguredAvroSerde;
import ts.information.resource.Key;

/**
 * Store for inf.resource (=entity) partitioned by pk_entity
 */
@ApplicationScoped
public class EStore extends AbstractStore<ts.information.resource.Key, EntityValue> {
    public static final String NAME = "e-store";
    @Inject
    ConfiguredAvroSerde as;

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Serde<Key> getKeySerde() {
        return as.key();
    }

    @Override
    public Serde<EntityValue> getValueSerde() {
        return as.value();
    }
}
