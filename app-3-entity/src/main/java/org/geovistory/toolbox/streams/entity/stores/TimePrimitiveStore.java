package org.geovistory.toolbox.streams.entity.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.avro.TimePrimitive;
import org.geovistory.toolbox.streams.entity.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;

/**
 * Store for inf.resource (=entity) partitioned by pk_entity
 */
@ApplicationScoped
public class TimePrimitiveStore extends AbstractStore<String, TimePrimitive> {
    public static final String NAME = "time-primitive-store";
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
    public Serde<TimePrimitive> getValueSerde() {
        return as.value();
    }
}
