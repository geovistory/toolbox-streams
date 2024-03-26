package org.geovistory.toolbox.streams.entity.label3.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.avro.LabelEdge;
import org.geovistory.toolbox.streams.entity.label3.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;

/**
 * Store for label edges partitioned by target entity id
 * with key: {fk_target}_{fk_project}_{...}
 * with val: LabelEdge
 */
@ApplicationScoped
public class LabelEdgeByTargetStore extends AbstractStore<String, LabelEdge> {
    public static final String NAME = "label-edge-by-target-store";
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
    public Serde<LabelEdge> getValueSerde() {
        return as.value();
    }

}
