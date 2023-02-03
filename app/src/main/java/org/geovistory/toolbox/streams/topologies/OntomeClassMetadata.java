package org.geovistory.toolbox.streams.topologies;

import dev.data_for_history.api_class.Key;
import dev.data_for_history.api_class.Value;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.app.DbTopicNames;
import org.geovistory.toolbox.streams.app.RegisterInputTopic;
import org.geovistory.toolbox.streams.avro.OntomeClassKey;
import org.geovistory.toolbox.streams.avro.OntomeClassMetadataValue;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.ArrayList;


public class OntomeClassMetadata {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {

        var register = new RegisterInputTopic(builder);

        var apiClassStream = register.dfhApiClassStream();

        return addProcessors(builder, apiClassStream).builder().build();

    }

    public static OntomeClassMetadataReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<Key, Value> apiClassStream
    ) {

        var avroSerdes = new ConfluentAvroSerdes();

        /* SOURCE PROCESSORS */

        // 2) group by dfh_pk_class
        var grouped = apiClassStream.groupBy(
                (key, value) -> OntomeClassKey.newBuilder().setClassId(value.getDfhPkClass()).build(),
                Grouped.with(
                        avroSerdes.OntomeClassKey(), avroSerdes.DfhApiClassValue()
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

        var stream = table.toStream();
        /* SINK PROCESSORS */
        stream
                .to(
                        output.TOPICS.ontome_class_metadata,
                        Produced.with(avroSerdes.OntomeClassKey(), avroSerdes.OntomeClassMetadataValue())
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
