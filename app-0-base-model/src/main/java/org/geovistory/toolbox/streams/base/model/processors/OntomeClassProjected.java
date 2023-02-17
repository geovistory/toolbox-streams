package org.geovistory.toolbox.streams.base.model.processors;

import dev.data_for_history.api_class.Key;
import dev.data_for_history.api_class.Value;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.geovistory.toolbox.streams.avro.OntomeClassKey;
import org.geovistory.toolbox.streams.avro.OntomeClassValue;
import org.geovistory.toolbox.streams.base.model.DbTopicNames;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.ProjectedTableRegistrar;

public class OntomeClassProjected extends ProjectedTableRegistrar<
        Key,
        Value,
        OntomeClassKey,
        OntomeClassValue
        > {
    public OntomeClassProjected(StreamsBuilder builder) {
        super(
                builder,
                // input topic name
                DbTopicNames.dfh_api_class.getName(),
                // input key serde
                new ConfluentAvroSerdes().DfhApiClassKey(),
                // input value serde
                new ConfluentAvroSerdes().DfhApiClassValue(),
                // prefix for outputs
                "ontome_class",
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
                new ConfluentAvroSerdes().OntomeClassKey(),
                // output value serde
                new ConfluentAvroSerdes().OntomeClassValue()
        );
    }
}
