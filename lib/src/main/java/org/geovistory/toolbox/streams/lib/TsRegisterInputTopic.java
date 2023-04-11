package org.geovistory.toolbox.streams.lib;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

abstract public class TsRegisterInputTopic {

    protected String prefix;

    static protected String prefixedIn(String prefix, String name) {
        return prefix + "." + name;
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
    protected <K, V> KTable<K, V> getRepartitionedTable(StreamsBuilder builder, String topicName, Serde<K> kSerde, Serde<V> vSerde) {

        return getStream(builder, topicName, kSerde, vSerde)
                .map(KeyValue::pair, Named.as(topicName + "-mark-for-repartition"))
                .toTable(
                        Named.as(topicName + "-to-table"),
                        Materialized
                                .<K, V, KeyValueStore<Bytes, byte[]>>as(topicName + "-store")
                                .withKeySerde(kSerde)
                                .withValueSerde(vSerde)
                );
    }


    protected <K, V> KStream<K, V> getStream(StreamsBuilder builder, String topicName, Serde<K> kSerde, Serde<V> vSerde) {
        return builder.stream(topicName, Consumed.with(kSerde, vSerde).withName(topicName + "-consumer"));
    }

    protected <K, V> KTable<K, V> getTable(StreamsBuilder builder, String topicName, Serde<K> kSerde, Serde<V> vSerde) {
        return builder.table(topicName, Consumed.with(kSerde, vSerde).withName(topicName + "-consumer"));
    }

    protected <K, V> KStream<K, V> getRepartitionedStream(StreamsBuilder builder, String topicName, Serde<K> kSerde, Serde<V> vSerde) {
        return getStream(builder, topicName, kSerde, vSerde)
                .map(KeyValue::pair, Named.as(topicName + "-mark-for-repartition"))
                .repartition(
                        Repartitioned.<K, V>as(topicName + "-repartitioned")
                                .withKeySerde(kSerde)
                                .withValueSerde(vSerde)
                );
    }
}
