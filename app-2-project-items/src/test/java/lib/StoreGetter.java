package lib;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;

public class StoreGetter {
    static public <K, V> ReadOnlyKeyValueStore<K, V> getStore(AbstractStore<K, V> storeHelper, KafkaStreams streams) {
        while (true) {
            try {
                return streams.store(StoreQueryParameters.fromNameAndType(storeHelper.getName(), QueryableStoreTypes.keyValueStore()));
            } catch (InvalidStateStoreException e) {
                // ignore, store not ready yet
            }
        }
    }

}
