package org.geovistory.toolbox.streams.entity.label3.stores;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.avro.ProjectClassKey;
import org.geovistory.toolbox.streams.entity.label3.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;

/**
 * Store for timestamps.
 * with key: String {classId}_{projectId}
 * with val: String {timestamp}
 */
@ApplicationScoped
public class LabelConfigTmstpStore extends AbstractStore<ProjectClassKey, Long> {
    public static final String NAME = "label-config-tmstp-store";
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
    public Serde<Long> getValueSerde() {
        return Serdes.Long();
    }

}
