package org.geovistory.toolbox.streams.entity.label3.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.geovistory.toolbox.streams.avro.EntityLabel;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.entity.label3.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;

/**
 * Store for EntityLabel.
 * with key: ProjectEntityKey
 * with val: EntityLabel
 */
@ApplicationScoped
public class EntityLabelStore extends AbstractStore<ProjectEntityKey, EntityLabel> {
    public static final String NAME = "entity-label-store";
    @Inject
    ConfiguredAvroSerde as;

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Serde<ProjectEntityKey> getKeySerde() {
        return as.key();
    }

    @Override
    public Serde<EntityLabel> getValueSerde() {
        return as.value();
    }

}
