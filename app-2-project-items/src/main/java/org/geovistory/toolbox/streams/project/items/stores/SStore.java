package org.geovistory.toolbox.streams.project.items.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;
import org.geovistory.toolbox.streams.project.items.lib.ConfiguredAvroSerde;

/**
 * Store for statements partitioned by pk_entity
 * with key: original
 * with val: IprJoinValue
 */
@ApplicationScoped
public class SStore extends AbstractStore<Integer, StatementEnrichedValue> {
    public static final String NAME = "s-store";
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
    public Serde<StatementEnrichedValue> getValueSerde() {
        return as.value();
    }


}
