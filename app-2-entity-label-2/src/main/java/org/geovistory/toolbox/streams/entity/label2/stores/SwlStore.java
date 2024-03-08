package org.geovistory.toolbox.streams.entity.label2.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.entity.label2.lib.AbstractStore;
import org.geovistory.toolbox.streams.entity.label2.lib.ConfiguredAvroSerde;

/**
 * Store for statements with literal partitioned by pk_entity
 * with key: original
 * with val: IprJoinValue
 */
@ApplicationScoped
public class SwlStore extends AbstractStore<Integer, StatementEnrichedValue> {
    public static final String NAME = "swl-store";
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
