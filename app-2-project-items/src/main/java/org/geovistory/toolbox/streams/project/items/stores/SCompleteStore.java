package org.geovistory.toolbox.streams.project.items.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.StatementJoinValue;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;
import org.geovistory.toolbox.streams.project.items.lib.ConfiguredAvroSerde;

/**
 * Store for statements partitioned by pk_entity
 * with key: original
 * with val: IprJoinValue
 */
@ApplicationScoped
public class SCompleteStore extends AbstractStore<ProjectStatementKey, StatementJoinValue> {
    public static final String NAME = "s-complete-store";
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
    public Serde<StatementJoinValue> getValueSerde() {
        return as.value();
    }


}
