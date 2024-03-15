package org.geovistory.toolbox.streams.project.items.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.avro.StatementWithObValue;
import org.geovistory.toolbox.streams.project.items.lib.AbstractStore;
import org.geovistory.toolbox.streams.project.items.lib.ConfiguredAvroSerde;

/**
 * Store for statements partitioned by subject
 * with key: {fk_entity}_{fk_project}
 * with val: StatementWithSubValue
 */
@ApplicationScoped
public class SObStore extends AbstractStore<String, StatementWithObValue> {
    public static final String NAME = "s-ob-store";
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
    public Serde<StatementWithObValue> getValueSerde() {
        return as.key();
    }

    /**
     * Create a String in the format of the key
     *
     * @return String in format {fk_entity}_{fk_project}_{statement_id}
     */
    public static String createKey(String entityId, int projectId, int statementId) {
        return entityId + "_" + projectId + "_" + statementId;
    }
}
