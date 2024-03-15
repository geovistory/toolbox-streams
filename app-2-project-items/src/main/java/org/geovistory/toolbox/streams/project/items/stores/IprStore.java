package org.geovistory.toolbox.streams.project.items.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.avro.IprJoinVal;
import org.geovistory.toolbox.streams.project.items.lib.AbstractStore;
import org.geovistory.toolbox.streams.project.items.lib.ConfiguredAvroSerde;

/**
 * Store for pro.info_proj_rel partitioned by fk_entity
 * with key: {fk_entity}_{fk_project}
 * with val: IprJoinValue
 */
@ApplicationScoped
public class IprStore extends AbstractStore<String, IprJoinVal> {
    public static final String NAME = "ipr-store";
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
    public Serde<IprJoinVal> getValueSerde() {
        return as.value();
    }

    /**
     * Create a String in the format of the key
     *
     * @param value input to transform
     * @return String in format {fk_entity}_{fk_project}
     */
    public static String createIprStoreKey(ts.projects.info_proj_rel.Value value) {
        return value.getFkEntity() + "_" + value.getFkProject();
    }

}
