package org.geovistory.toolbox.streams.base.model.processors;

import org.apache.kafka.streams.KeyValue;
import org.geovistory.toolbox.streams.avro.OntomePropertyKey;
import org.geovistory.toolbox.streams.avro.OntomePropertyValue;
import org.geovistory.toolbox.streams.base.model.AvroSerdes;
import org.geovistory.toolbox.streams.base.model.BuilderSingleton;
import org.geovistory.toolbox.streams.base.model.InputTopicNames;
import org.geovistory.toolbox.streams.base.model.OutputTopicNames;
import org.geovistory.toolbox.streams.lib.ProjectedTableRegistrar;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped

public class OntomePropertyProjected {

    @Inject
    AvroSerdes avroSerdes;
    @Inject
    BuilderSingleton builderSingleton;
    @Inject
    InputTopicNames inputTopicNames;
    @Inject
    OutputTopicNames outputTopicNames;

    private ProjectedTableRegistrar<
            dev.data_for_history.api_property.Key,
            dev.data_for_history.api_property.Value,
            OntomePropertyKey,
            OntomePropertyValue
            > registrar;


    public ProjectedTableRegistrar<
            dev.data_for_history.api_property.Key,
            dev.data_for_history.api_property.Value,
            OntomePropertyKey,
            OntomePropertyValue
            > getRegistrar(AvroSerdes avroSerdes, BuilderSingleton builderSingleton, InputTopicNames inputTopicNames, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.builderSingleton = builderSingleton;
        this.inputTopicNames = inputTopicNames;
        this.outputTopicNames = outputTopicNames;
        return getRegistrar();
    }

    public ProjectedTableRegistrar<
            dev.data_for_history.api_property.Key,
            dev.data_for_history.api_property.Value,
            OntomePropertyKey,
            OntomePropertyValue
            > getRegistrar() {
        if (registrar == null) {

            registrar = new ProjectedTableRegistrar<>(
                    builderSingleton.builder,
                    // input topic name
                    inputTopicNames.dfhApiProperty(),
                    // input key serde
                    avroSerdes.DfhApiPropertyKey(),
                    // input value serde
                    avroSerdes.DfhApiPropertyValue(),
                    // prefix for outputs
                    outputTopicNames.ontomeProperty(),
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
        return registrar;
    }
}
