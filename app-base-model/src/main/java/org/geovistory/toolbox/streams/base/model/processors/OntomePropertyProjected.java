package org.geovistory.toolbox.streams.base.model.processors;

import dev.data_for_history.api_property.Key;
import dev.data_for_history.api_property.Value;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.geovistory.toolbox.streams.avro.OntomePropertyKey;
import org.geovistory.toolbox.streams.avro.OntomePropertyValue;
import org.geovistory.toolbox.streams.base.model.DbTopicNames;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.ProjectedTableRegistrar;
import org.geovistory.toolbox.streams.lib.Utils;

public class OntomePropertyProjected extends ProjectedTableRegistrar<
        Key,
        Value,
        OntomePropertyKey,
        OntomePropertyValue
        > {
    public OntomePropertyProjected(StreamsBuilder builder) {
        super(
                builder,
                // input topic name
                DbTopicNames.dfh_api_property.getName(),
                // input key serde
                new ConfluentAvroSerdes().DfhApiPropertyKey(),
                // input value serde
                new ConfluentAvroSerdes().DfhApiPropertyValue(),
                // prefix for outputs
                Utils.tsPrefixed("ontome_property"),
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
                new ConfluentAvroSerdes().OntomePropertyKey(),
                // output value serde
                new ConfluentAvroSerdes().OntomePropertyValue()
        );
    }
}
