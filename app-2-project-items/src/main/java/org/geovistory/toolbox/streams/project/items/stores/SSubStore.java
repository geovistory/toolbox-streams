package org.geovistory.toolbox.streams.project.items.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.StatementValue;
import org.geovistory.toolbox.streams.avro.StatementWithSubValue;
import org.geovistory.toolbox.streams.project.items.lib.AbstractStore;
import org.geovistory.toolbox.streams.project.items.lib.ConfiguredAvroSerde;

/**
 * Store for statements partitioned by subject
 * with key: {fk_entity}_{fk_project}
 * with val: StatementWithSubValue
 */
@ApplicationScoped
public class SSubStore extends AbstractStore<String, StatementWithSubValue> {
    public static final String NAME = "s-sub-store";
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
    public Serde<StatementWithSubValue> getValueSerde() {
        return as.value();
    }

    /**
     * Create a String in the format of the key
     *
     * @param k input to transform
     * @return String in format {fk_entity}_{fk_project}_{statment_id}
     */
    public static String createKey(ProjectEntityKey k, StatementValue v) {
        return k.getEntityId() + "_" + k.getProjectId() + "_" + v.getStatementId();
    }
}
