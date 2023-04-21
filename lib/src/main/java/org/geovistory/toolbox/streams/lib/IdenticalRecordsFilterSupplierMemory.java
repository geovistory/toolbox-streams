package org.geovistory.toolbox.streams.lib;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Set;

public class IdenticalRecordsFilterSupplierMemory<K, V> implements TransformerSupplier<K, V, KeyValue<K, V>> {

    private final String stateStoreName;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    public IdenticalRecordsFilterSupplierMemory(String stateStoreName, Serde<K> keySerde, Serde<V> valueSerde) {
        this.stateStoreName = stateStoreName;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    public Transformer<K, V, KeyValue<K, V>> get() {
        return new IdenticalRecordsFilter<>(stateStoreName);
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        StoreBuilder<KeyValueStore<K, V>> keyValueStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(stateStoreName),
                        keySerde,
                        valueSerde);
        return Collections.singleton(keyValueStoreBuilder);
    }
}
