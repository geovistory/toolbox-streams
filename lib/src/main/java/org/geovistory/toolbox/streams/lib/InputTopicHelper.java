package org.geovistory.toolbox.streams.lib;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashMap;
import java.util.Map;

abstract public class InputTopicHelper {

    public StreamsBuilder builder;

    public Map<String, KStream<?, ?>> streamCache = new HashMap<>();

    public InputTopicHelper(StreamsBuilder builder) {
        this.builder = builder;
    }

    /**
     * Register a KStream and map it to a new KTable to ensure proper partitioning
     * This seems to be needed to correctly join topics created by debezium, since they have
     * a different Partitioner then this Kafka Streams application.
     *
     * @param topicName name of topic to consume from
     * @param kSerde    key Serde
     * @param vSerde    value Serde
     * @return KTable
     */
    protected <K, V> KTable<K, V> getRepartitionedTable(String topicName, Serde<K> kSerde, Serde<V> vSerde) {

        return getStream(topicName, kSerde, vSerde)
                .map(KeyValue::pair, Named.as(topicName + "-mark-for-repartition"))
                .toTable(
                        Named.as(topicName + "-to-table"),
                        Materialized
                                .<K, V, KeyValueStore<Bytes, byte[]>>as(topicName + "-store")
                                .withKeySerde(kSerde)
                                .withValueSerde(vSerde)
                );
    }


    protected <K, V> KStream<K, V> getStream(String topicName, Serde<K> kSerde, Serde<V> vSerde) {
        if (streamCache.containsKey(topicName)) {
            return (KStream<K, V>) streamCache.get(topicName);
        }
        var s = builder.stream(topicName, Consumed.with(kSerde, vSerde).withName(topicName + "-consumer"));
        this.streamCache.put(topicName, s);
        return s;
    }

    protected <K, V> KTable<K, V> getTable(String topicName, Serde<K> kSerde, Serde<V> vSerde) {
        return getStream(topicName, kSerde, vSerde).toTable(
                Named.as(topicName + "-to-table"),
                Materialized
                        .<K, V, KeyValueStore<Bytes, byte[]>>as(topicName + "-store")
                        .withKeySerde(kSerde)
                        .withValueSerde(vSerde)
        );
    }

    protected <K, V> KStream<K, V> getRepartitionedStream(String topicName, Serde<K> kSerde, Serde<V> vSerde) {
        return getStream(topicName, kSerde, vSerde)
                .map(KeyValue::pair, Named.as(topicName + "-mark-for-repartition"))
                .repartition(
                        Repartitioned.<K, V>as(topicName + "-repartitioned")
                                .withKeySerde(kSerde)
                                .withValueSerde(vSerde)
                );
    }
}
