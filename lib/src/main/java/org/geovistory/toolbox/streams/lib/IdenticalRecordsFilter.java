package org.geovistory.toolbox.streams.lib;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Objects;

public class IdenticalRecordsFilter<K, V> implements Transformer<K, V, KeyValue<K, V>> {

    private final String stateStoreName;
    private KeyValueStore<K, V> kvStore;

    public IdenticalRecordsFilter(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.kvStore = context.getStateStore(stateStoreName);
    }

    @Override
    public KeyValue<K, V> transform(
            K key,
            V value
    ) {
        var oldValue = kvStore.get(key);

        // if the new value equals the old value, stop propagation to downstream processors
        if (Objects.equals(oldValue, value)) return null;

        // else propagate to downstream processor
        kvStore.put(key, value);
        return KeyValue.pair(key, value);

    }

    public void close() {

    }

}

