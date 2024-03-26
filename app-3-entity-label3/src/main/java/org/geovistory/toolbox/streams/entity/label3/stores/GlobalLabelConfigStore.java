package org.geovistory.toolbox.streams.entity.label3.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.geovistory.toolbox.streams.avro.EntityLabelConfigTmstp;
import org.geovistory.toolbox.streams.avro.ProjectClassKey;
import org.geovistory.toolbox.streams.entity.label3.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;

/**
 * Store for ts.projects.entity_label_config
 */
@ApplicationScoped
public class GlobalLabelConfigStore extends AbstractStore<ProjectClassKey, EntityLabelConfigTmstp> {
    public static final String NAME = "global-label-config-store";
    @Inject
    ConfiguredAvroSerde as;

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Serde<ProjectClassKey> getKeySerde() {
        return as.key();
    }

    @Override
    public Serde<EntityLabelConfigTmstp> getValueSerde() {
        return as.value();
    }

}
