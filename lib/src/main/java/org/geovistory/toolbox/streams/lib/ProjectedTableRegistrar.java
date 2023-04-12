package org.geovistory.toolbox.streams.lib;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

/**
 * Class to project an input topic to an output topic, ensuring that only
 * real updates on the output topic get forwarded downstream:
 * If an out record value is identical to the latest stored value with same key,
 * the record is not forwarded downstream. The class has two constructors:
 * - one to first build an input KStream from given topic name.
 * - one to project an input KStream directly.
 *
 * <p>
 * The output topic
 * - contains projected / (mapped) values,
 * - contains only updates of identical key-value records (records with the same key value is ignored)
 */
 public class ProjectedTableRegistrar<InputKey, InputValue, OutputKey, OutputValue> {
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
     * @param builder         StreamsBuilder to use for the creation of KStream/KTable
     * @param inputTopicName  name of input topic
     * @param inputKeySerde   key Serde of input topic
     * @param inputValueSerde value Serde of input topic
     * @param baseName base name for processors, store, and topics
     * @param keyValueMapper maps input records to output records
     * @param outputKeySerde   key Serde of output topic
     * @param outputValueSerde value Serde of output topic
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
        var inputStream = getInputStream();
        init(inputStream);
    }


    /**
     * @param builder         StreamsBuilder to use for the creation of KStream/KTable
     * @param inputStream  KStream to project
     * @param baseName base name for processors, store, and topics
     * @param keyValueMapper maps input records to output records
     * @param outputKeySerde   key Serde of output topic
     * @param outputValueSerde value Serde of output topic
     */
    public ProjectedTableRegistrar(
            StreamsBuilder builder,
            KStream<InputKey,InputValue> inputStream,
            String baseName,
            KeyValueMapper<InputKey, InputValue, KeyValue<OutputKey, OutputValue>> keyValueMapper,
            Serde<OutputKey> outputKeySerde,
            Serde<OutputValue> outputValueSerde
    ) {
        this.builder = builder;
        this.baseName = baseName;
        this.keyValueMapper = keyValueMapper;
        this.outputKeySerde = outputKeySerde;
        this.outputValueSerde = outputValueSerde;
        init(inputStream);
    }

    private KStream<InputKey, InputValue> getInputStream() {
        return builder.stream(
                inputTopicName,
                Consumed.with(inputKeySerde, inputValueSerde)
                        .withName(baseName + "-source")
        );
    }

    /**
     * Initialize a KTable with properly repartitioned messages.
     */
    private void init(KStream<InputKey, InputValue> inputStream) {
        var processorMap = baseName + "-projection";

        var processorRepartition = baseName + "-repartition";

        var processorTransform = baseName + "-suppress-duplicates";
        var storeTransform = baseName + "-suppress-duplicates";

        var processorToTable = baseName + "-table";

        this.storeToTable = processorToTable + "-store";


        kStream = inputStream
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
