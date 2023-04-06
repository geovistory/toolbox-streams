package org.geovistory.toolbox.streams.base.model.processors;

import dev.data_for_history.api_class.Key;
import dev.data_for_history.api_class.Value;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.geovistory.toolbox.streams.avro.OntomeClassKey;
import org.geovistory.toolbox.streams.avro.OntomeClassValue;
import org.geovistory.toolbox.streams.base.model.AvroSerdes;
import org.geovistory.toolbox.streams.lib.ProjectedTableRegistrar;

public class OntomeClassProjected extends ProjectedTableRegistrar<
        Key,
        Value,
        OntomeClassKey,
        OntomeClassValue
        > {


    public OntomeClassProjected(AvroSerdes avroSerdes, StreamsBuilder builder, String inputTopicName, String outputTopicName) {
        super(
                builder,
                // input topic name
                inputTopicName,
                // input key serde
                avroSerdes.DfhApiClassKey(),
                // input value serde
                avroSerdes.DfhApiClassValue(),
                // prefix for outputs
                outputTopicName,
                (key, value) -> KeyValue.pair(
                        OntomeClassKey.newBuilder().setClassId(value.getDfhPkClass()).build(),
                        OntomeClassValue.newBuilder()
                                .setDfhPkClass(value.getDfhPkClass())
                                .setDfhFkProfile(value.getDfhFkProfile())
                                .setDfhParentClasses(value.getDfhParentClasses())
                                .setDfhAncestorClasses(value.getDfhAncestorClasses())
                                .setDfhClassLabel(value.getDfhClassLabel())
                                .setDfhClassLabelLanguage(value.getDfhClassLabelLanguage())
                                .setRemovedFromApi(value.getRemovedFromApi())
                                .setDeleted$1(value.getDeleted$1())
                                .build()
                ),
                // output key serde
                avroSerdes.OntomeClassKey(),
                // output value serde
                avroSerdes.OntomeClassValue()
        );
    }
}
