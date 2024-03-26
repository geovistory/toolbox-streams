package org.geovistory.toolbox.streams.entity.label3.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.avro.ComLabelGroupKey;
import org.geovistory.toolbox.streams.entity.label3.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;

/**
 * Store for grouping and counting community labels.
 * with key: ComLabelGroupKey consisting of entity_id, label and language
 * with val: int, count of project entity labels per group
 */
@ApplicationScoped
public class ComLabelCountStore extends AbstractStore<ComLabelGroupKey, Integer> {
    public static final String NAME = "com-label-count-store";
    @Inject
    ConfiguredAvroSerde as;

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Serde<ComLabelGroupKey> getKeySerde() {
        return as.key();
    }

    @Override
    public Serde<Integer> getValueSerde() {
        return Serdes.Integer();
    }

}
