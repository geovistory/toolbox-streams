package org.geovistory.toolbox.streams.entity.label2.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.geovistory.toolbox.streams.avro.EntityValue;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.entity.label2.lib.AbstractStore;
import org.geovistory.toolbox.streams.entity.label2.lib.ConfiguredAvroSerde;

/**
 * Store for statements partitioned by subject
 * with key: {fk_entity}_{fk_project}
 * with val: StatementWithSubValue
 */
@ApplicationScoped
public class ObByStmtStore extends AbstractStore<ProjectStatementKey, EntityValue> {
    public static final String NAME = "ob-by-s-store";
    @Inject
    ConfiguredAvroSerde as;

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Serde<ProjectStatementKey> getKeySerde() {
        return as.key();
    }

    @Override
    public Serde<EntityValue> getValueSerde() {
        return as.key();
    }

}
