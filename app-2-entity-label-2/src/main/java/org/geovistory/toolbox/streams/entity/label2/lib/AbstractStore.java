package org.geovistory.toolbox.streams.entity.label2.lib;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * Abstract class to streamline management of Stores
 * Extend this class with an @ApplicationScoped class
 * to configure a state store
 *
 * @param <K>
 * @param <V>
 */
public abstract class AbstractStore<K, V> {
    abstract public String getName();

    abstract public Serde<K> getKeySerde();

    abstract public Serde<V> getValueSerde();

    public StoreBuilder<KeyValueStore<K, V>> createPersistentKeyValueStore() {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(getName()),
                getKeySerde(),
                getValueSerde()
        );
    }

}