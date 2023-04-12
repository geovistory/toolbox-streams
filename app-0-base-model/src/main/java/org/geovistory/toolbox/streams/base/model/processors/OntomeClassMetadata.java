package org.geovistory.toolbox.streams.base.model.processors;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.OntomeClassKey;
import org.geovistory.toolbox.streams.avro.OntomeClassMetadataValue;
import org.geovistory.toolbox.streams.avro.OntomeClassValue;
import org.geovistory.toolbox.streams.base.model.AvroSerdes;
import org.geovistory.toolbox.streams.base.model.BuilderSingleton;
import org.geovistory.toolbox.streams.base.model.InputTopicNames;
import org.geovistory.toolbox.streams.base.model.OutputTopicNames;
import org.geovistory.toolbox.streams.lib.TopicNameEnum;
import org.geovistory.toolbox.streams.lib.Utils;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.ArrayList;


@ApplicationScoped
public class OntomeClassMetadata {

    @Inject
    AvroSerdes avroSerdes;

    @ConfigProperty(name = "ts.input.topic.name.prefix", defaultValue = "")
    String inPrefix;
    @ConfigProperty(name = "ts.output.topic.name.prefix", defaultValue = "")
    public String outPrefix;
    @Inject
    BuilderSingleton builderSingleton;

    @Inject
    InputTopicNames inputTopicNames;

    @Inject
    OutputTopicNames outputTopicNames;

    public OntomeClassMetadata(AvroSerdes avroSerdes, BuilderSingleton builderSingleton, InputTopicNames inputTopicNames, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.builderSingleton = builderSingleton;
        this.inputTopicNames = inputTopicNames;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
        addProcessors(
                new OntomeClassProjected().getRegistrar(
                        this.avroSerdes, this.builderSingleton, this.inputTopicNames, this.outputTopicNames
                ).kStream
        );
    }

    public OntomeClassMetadataReturnValue addProcessors(
            KStream<OntomeClassKey, OntomeClassValue> ontomeClassStream
    ) {

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
                        outOntomeClassMetadata(),
                        Produced.with(avroSerdes.OntomeClassKey(), avroSerdes.OntomeClassMetadataValue())
                                .withName(outOntomeClassMetadata() + "-producer")
                );


        return new OntomeClassMetadataReturnValue(stream, table);

    }


    public enum inner {
        TOPICS;
        public final String ontome_class_metadata_grouped = "ontome_class_metadata_grouped";
        public final String ontome_class_metadata_aggregated = "ontome_class_metadata_aggregated";

    }

    public String inApiClass() {
        return Utils.prefixedIn(inPrefix, TopicNameEnum.dfh_api_class.getValue());
    }


    public String outOntomeClassMetadata() {
        return Utils.prefixedOut(outPrefix, "ontome_class_metadata");
    }


}
