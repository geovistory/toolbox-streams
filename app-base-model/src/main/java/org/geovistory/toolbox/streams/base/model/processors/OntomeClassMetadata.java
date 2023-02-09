package org.geovistory.toolbox.streams.base.model.processors;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.OntomeClassKey;
import org.geovistory.toolbox.streams.avro.OntomeClassMetadataValue;
import org.geovistory.toolbox.streams.avro.OntomeClassValue;
import org.geovistory.toolbox.streams.base.model.DbTopicNames;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.ArrayList;


public class OntomeClassMetadata {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {


        var ontomeClassStream = new OntomeClassProjected(builder).kStream;

        return addProcessors(builder, ontomeClassStream).builder().build();

    }

    public static OntomeClassMetadataReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<OntomeClassKey, OntomeClassValue> ontomeClassStream
    ) {

        var avroSerdes = new ConfluentAvroSerdes();

        /* SOURCE PROCESSORS */

        // 2) group by dfh_pk_class
        var grouped = ontomeClassStream.groupByKey(
                Grouped.with(
                        avroSerdes.OntomeClassKey(), avroSerdes.OntomeClassValue()
                ).withName(inner.TOPICS.ontome_class_metadata_grouped)
        );
        // 2) aggregate OntomeClassMetadataValue
        var table = grouped.aggregate(
                () -> OntomeClassMetadataValue.newBuilder()
                        .setParentClasses(new ArrayList<>())
                        .setAncestorClasses(new ArrayList<>())
                        .build(),
                (key, value, aggregate) -> {
                    aggregate.setAncestorClasses(value.getDfhAncestorClasses());
                    aggregate.setParentClasses(value.getDfhParentClasses());
                    return aggregate;
                },
                Materialized.<OntomeClassKey, OntomeClassMetadataValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.ontome_class_metadata_aggregated) /* state store name */
                        .withKeySerde(avroSerdes.OntomeClassKey())
                        .withValueSerde(avroSerdes.OntomeClassMetadataValue())
        );

        var stream = table.toStream(
                Named.as(inner.TOPICS.ontome_class_metadata_aggregated + "-to-stream")
        );
        /* SINK PROCESSORS */
        stream
                .to(
                        output.TOPICS.ontome_class_metadata,
                        Produced.with(avroSerdes.OntomeClassKey(), avroSerdes.OntomeClassMetadataValue())
                                .withName(output.TOPICS.ontome_class_metadata + "-producer")
                );


        return new OntomeClassMetadataReturnValue(builder, stream, table);

    }


    public enum input {
        TOPICS;
        public final String api_class = DbTopicNames.dfh_api_class.getName();


    }

    public enum inner {
        TOPICS;
        public final String ontome_class_metadata_grouped = "ontome_class_metadata_grouped";
        public final String ontome_class_metadata_aggregated = "ontome_class_metadata_aggregated";


    }


    public enum output {
        TOPICS;
        public final String ontome_class_metadata = Utils.tsPrefixed("ontome_class_metadata");

    }

}
