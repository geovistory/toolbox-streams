package org.geovistory.toolbox.streams.entity.label.processsors.community;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;

import java.util.Collections;
import java.util.Set;

public abstract class CommunityStatementCounterSupplier implements TransformerSupplier<
        ProjectStatementKey, ProjectStatementValue,
        KeyValue<CommunityStatementKey, CommunityStatementValue>> {

    protected final String stateStoreName;
    private final ConfluentAvroSerdes avroSerdes = new ConfluentAvroSerdes();


    CommunityStatementCounterSupplier(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public abstract Transformer<ProjectStatementKey, ProjectStatementValue, KeyValue<CommunityStatementKey, CommunityStatementValue>> get();

    @Override
    public Set<StoreBuilder<?>> stores() {
        StoreBuilder<KeyValueStore<CommunityStatementKey, DRMap>> keyValueStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
                        avroSerdes.CommunityStatementKey(),
                        avroSerdes.DRMap());
        return Collections.singleton(keyValueStoreBuilder);
    }
}
