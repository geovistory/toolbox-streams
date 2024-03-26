package org.geovistory.toolbox.streams.entity.label3.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.avro.EntityLabel;
import org.geovistory.toolbox.streams.entity.label3.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;

/**
 * Store for ranking the community language labels by their count, used to create
 * the preferred community label of an entity per language.
 * <p>
 * with key: String of this form {entity_id}_{label}_{1/count}_{language}
 * with val: EntityLabel containing label and language
 */
@ApplicationScoped
public class ComLabelLangRankStore extends AbstractStore<String, EntityLabel> {
    public static final String NAME = "com-label-lang-rank-store";
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
    public Serde<EntityLabel> getValueSerde() {
        return as.value();
    }

}
