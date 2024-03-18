package org.geovistory.toolbox.streams.entity.label3.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.geovistory.toolbox.streams.entity.label3.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;

/**
 * Store for ts.projects.entity_label_config
 */
@ApplicationScoped
public class GlobalLabelConfigStore extends AbstractStore<ts.projects.entity_label_config.Key, ts.projects.entity_label_config.Value> {
    public static final String NAME = "global-edge-store";
    @Inject
    ConfiguredAvroSerde as;

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Serde<ts.projects.entity_label_config.Key> getKeySerde() {
        return as.key();
    }

    @Override
    public Serde<ts.projects.entity_label_config.Value> getValueSerde() {
        return as.value();
    }

}
