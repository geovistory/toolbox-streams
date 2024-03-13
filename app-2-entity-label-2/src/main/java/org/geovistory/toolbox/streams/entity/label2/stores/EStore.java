package org.geovistory.toolbox.streams.entity.label2.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.avro.EntityProjectedValue;
import org.geovistory.toolbox.streams.entity.label2.lib.AbstractStore;
import org.geovistory.toolbox.streams.entity.label2.lib.ConfiguredAvroSerde;

/**
 * Store for inf.resource (=entity) partitioned by pk_entity
 */
@ApplicationScoped
public class EStore extends AbstractStore<Integer, EntityProjectedValue> {
    public static final String NAME = "e-store";
    @Inject
    ConfiguredAvroSerde as;

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Serde<Integer> getKeySerde() {
        return Serdes.Integer();
    }

    @Override
    public Serde<EntityProjectedValue> getValueSerde() {
        return as.value();
    }
}