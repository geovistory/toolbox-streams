package org.geovistory.toolbox.streams.project.items.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.geovistory.toolbox.streams.avro.EntityValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;
import org.geovistory.toolbox.streams.project.items.lib.ConfiguredAvroSerde;

/**
 * Store for EntityValue partitioned by ProjectEntityKey
 */
@ApplicationScoped
public class PEStore extends AbstractStore<ProjectEntityKey, EntityValue> {
    public static final String NAME = "pe-store";
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
    public Serde<EntityValue> getValueSerde() {
        return as.value();
    }
}
