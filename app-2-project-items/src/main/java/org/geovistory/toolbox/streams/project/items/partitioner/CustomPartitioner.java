package org.geovistory.toolbox.streams.project.items.partitioner;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.geovistory.toolbox.streams.project.items.lib.ConfiguredAvroSerde;

import java.util.function.Function;

/**
 * A custom partitioner implementation for Kafka Streams, which allows partitioning based on a callback function.
 *
 * @param <KeyIn>        the type of keys for incoming records
 * @param <ValIn>        the type of values for incoming records
 * @param <PartitionKey> the type of partition keys produced by the callback function, extending Avro SpecificRecord
 */
public class CustomPartitioner<KeyIn, ValIn, PartitionKey extends SpecificRecord> implements StreamPartitioner<KeyIn, ValIn> {
    ConfiguredAvroSerde as;
    Function<KeyValue<KeyIn, ValIn>, PartitionKey> callback;

    /**
     * Constructs a new CustomPartitioner.
     *
     * @param as       the ConfiguredAvroSerde instance for serialization
     * @param callback the callback function that generates partition keys based on incoming records.
     *                 the key returned by the callback has to be an Avro instance.
     */
    public CustomPartitioner(ConfiguredAvroSerde as, Function<KeyValue<KeyIn, ValIn>, PartitionKey> callback) {
        this.as = as;
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

        try (var k = as.<PartitionKey>key()) {
            final byte[] keyBytes = k.serializer().serialize(topic, null, newKey);
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


}