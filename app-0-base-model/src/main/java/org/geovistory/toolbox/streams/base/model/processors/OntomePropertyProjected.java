package org.geovistory.toolbox.streams.base.model.processors;

import dev.data_for_history.api_property.Key;
import dev.data_for_history.api_property.Value;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.geovistory.toolbox.streams.avro.OntomePropertyKey;
import org.geovistory.toolbox.streams.avro.OntomePropertyValue;
import org.geovistory.toolbox.streams.base.model.AvroSerdes;
import org.geovistory.toolbox.streams.lib.ProjectedTableRegistrar;

public class OntomePropertyProjected extends ProjectedTableRegistrar<
        Key,
        Value,
        OntomePropertyKey,
        OntomePropertyValue
        > {
    public OntomePropertyProjected(AvroSerdes avroSerdes, StreamsBuilder builder, String inputTopicName, String outputTopicName) {
        super(
                builder,
                // input topic name
                inputTopicName,
                // input key serde
                avroSerdes.DfhApiPropertyKey(),
                // input value serde
                avroSerdes.DfhApiPropertyValue(),
                // prefix for outputs
                outputTopicName,
                (key, value) -> KeyValue.pair(
                        OntomePropertyKey.newBuilder().setPropertyId(value.getDfhPkProperty()).build(),
                        OntomePropertyValue.newBuilder()
                                .setDfhPkProperty(value.getDfhPkProperty())
                                .setDfhPropertyDomain(value.getDfhPropertyDomain())
                                .setDfhPropertyRange(value.getDfhPropertyRange())
                                .setDfhFkProfile(value.getDfhFkProfile())
                                .setDfhParentProperties(value.getDfhParentProperties())
                                .setDfhAncestorProperties(value.getDfhAncestorProperties())
                                .setDfhPropertyLabel(value.getDfhPropertyLabel())
                                .setDfhPropertyInverseLabel(value.getDfhPropertyInverseLabel())
                                .setDfhPropertyLabelLanguage(value.getDfhPropertyLabelLanguage())
                                .setRemovedFromApi(value.getRemovedFromApi())
                                .setDeleted$1(value.getDeleted$1())
                                .build()
                ),
                // output key serde
                avroSerdes.OntomePropertyKey(),
                // output value serde
                avroSerdes.OntomePropertyValue()
        );
    }
}
