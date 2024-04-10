package org.geovistory.toolbox.streams.entity.label3.partitioner;

import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.function.Function;

/**
 * A custom partitioner implementation for Kafka Streams, which allows partitioning based on a callback function.
 *
 * @param <KeyIn>        the type of keys for incoming records
 * @param <ValIn>        the type of values for incoming records
 * @param <PartitionKey> the type of partition keys produced by the callback function, extending Avro SpecificRecord
 */
public class CustomPartitioner<KeyIn, ValIn, PartitionKey> implements StreamPartitioner<KeyIn, ValIn> {
    Serializer<PartitionKey> keySerializer;
    Function<KeyValue<KeyIn, ValIn>, PartitionKey> callback;

    /**
     * Constructs a new CustomPartitioner.
     *
     * @param keySerializer the Serializer<PartitionKey> instance for serialization of the new key
     * @param callback      the callback function that generates partition keys based on incoming records.
     *                      the key returned by the callback has to be an Avro instance.
     */
    public CustomPartitioner(Serializer<PartitionKey> keySerializer, Function<KeyValue<KeyIn, ValIn>, PartitionKey> callback) {
        this.keySerializer = keySerializer;
        this.callback = callback;
    }

    /**
     * Partitions incoming records based on the provided callback function.
     *
     * @param topic         the topic name
     * @param key           the key of the incoming record
     * @param value         the value of the incoming record
     * @param numPartitions the total number of partitions in the topic
     * @return the partition number for the record, or null if partitioning should be determined by the producer
     */
    @Override
    @Deprecated
    public Integer partition(final String topic, final KeyIn key, final ValIn value, final int numPartitions) {

        // create the key for partitioning
        var newKey = callback.apply(KeyValue.pair(key, value));

        final byte[] keyBytes = keySerializer.serialize(topic, null, newKey);
        // if the key bytes are not available, we just return null to let the producer to decide
        // which partition to send internally; otherwise stick with the same built-in partitioner
        // util functions that producer used to make sure its behavior is consistent with the producer
        if (keyBytes == null) {
            return null;
        } else {
            return BuiltInPartitioner.partitionForKey(keyBytes, numPartitions);
        }
    }


}