package org.geovistory.toolbox.streams.utils;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

/**
 * Class to streamline the way of consuming
 * input topics. With input topics we mean topics not
 * produced by toolbox streams like for example
 * topics created by debezium.
 * <p>
 * The output topic
 * - contains projected / (mapped) values,
 * - contains only updates of identical key-value records (records with the same key value is ignored)
 */
abstract public class ProjectedTableRegistrar<InputKey, InputValue, OutputKey, OutputValue> {
    public StreamsBuilder builder;
    public String inputTopicName;
    public Serde<InputKey> inputKeySerde;
    public Serde<InputValue> inputValueSerde;
    public final String baseName;
    public String storeToTable;
    public String outputTopicName;
    KeyValueMapper<InputKey, InputValue, KeyValue<OutputKey, OutputValue>> keyValueMapper;
    public Serde<OutputKey> outputKeySerde;
    public Serde<OutputValue> outputValueSerde;
    public KStream<OutputKey, OutputValue> kStream;


    /**
     * @param inputTopicName  name of topic to consume
     * @param builder         StreamsBuilder to use for the creation of KStream/KTable
     * @param inputKeySerde   key Serde
     * @param inputValueSerde value Serde
     */
    public ProjectedTableRegistrar(
            StreamsBuilder builder,
            String inputTopicName,
            Serde<InputKey> inputKeySerde,
            Serde<InputValue> inputValueSerde,
            String baseName,
            KeyValueMapper<InputKey, InputValue, KeyValue<OutputKey, OutputValue>> keyValueMapper,
            Serde<OutputKey> outputKeySerde,
            Serde<OutputValue> outputValueSerde
    ) {
        this.builder = builder;
        this.inputTopicName = inputTopicName;
        this.inputKeySerde = inputKeySerde;
        this.inputValueSerde = inputValueSerde;
        this.baseName = baseName;
        this.keyValueMapper = keyValueMapper;
        this.outputKeySerde = outputKeySerde;
        this.outputValueSerde = outputValueSerde;
        init();

    }

    /**
     * Initialize a KTable with properly repartitioned messages.
     */
    private void init() {
        var processorMap = baseName + "-projection";

        var processorRepartition = baseName + "-repartition";

        var processorTransform = baseName + "-suppress-duplicates";
        var storeTransform = baseName + "-suppress-duplicates";

        var processorToTable = baseName + "-table";

        this.storeToTable = processorToTable + "-store";


        kStream = builder.stream(
                        inputTopicName,
                        Consumed.with(inputKeySerde, inputValueSerde)
                                .withName(baseName + "-source")
                )
                .map(keyValueMapper, Named.as(processorMap))
                .repartition(
                        Repartitioned.<OutputKey, OutputValue>as(processorRepartition)
                                .withKeySerde(outputKeySerde)
                                .withValueSerde(outputValueSerde)
                )
                .transform(
                        new IdenticalRecordsFilterSupplier<>(storeTransform, outputKeySerde, outputValueSerde),
                        Named.as(processorTransform)
                );


    }

    public void addSink() {

        var processorTo = baseName + "";
        this.outputTopicName = processorTo;

        kStream.to(
                processorTo,
                Produced.as(processorTo)
                        .with(outputKeySerde, outputValueSerde)
        );

    }


}
