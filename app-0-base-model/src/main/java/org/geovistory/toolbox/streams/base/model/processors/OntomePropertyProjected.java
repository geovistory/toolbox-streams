package org.geovistory.toolbox.streams.base.model.processors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.streams.KeyValue;
import org.geovistory.toolbox.streams.avro.OntomePropertyKey;
import org.geovistory.toolbox.streams.avro.OntomePropertyValue;
import org.geovistory.toolbox.streams.base.model.BuilderSingleton;
import org.geovistory.toolbox.streams.base.model.InputTopicNames;
import org.geovistory.toolbox.streams.base.model.OutputTopicNames;
import org.geovistory.toolbox.streams.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.lib.ProjectedTableRegistrar;

@ApplicationScoped

public class OntomePropertyProjected {

    @Inject
    ConfiguredAvroSerde as;
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
            > getRegistrar(ConfiguredAvroSerde as, BuilderSingleton builderSingleton, InputTopicNames inputTopicNames, OutputTopicNames outputTopicNames) {
        this.as = as;
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
                    as.key(),
                    // input value serde
                    as.value(),
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
                    as.key(),
                    // output value serde
                    as.value()
            );
        }
        return registrar;
    }
}
