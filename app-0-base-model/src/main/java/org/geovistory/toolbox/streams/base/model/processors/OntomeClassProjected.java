package org.geovistory.toolbox.streams.base.model.processors;

import org.apache.kafka.streams.KeyValue;
import org.geovistory.toolbox.streams.avro.OntomeClassKey;
import org.geovistory.toolbox.streams.avro.OntomeClassValue;
import org.geovistory.toolbox.streams.base.model.AvroSerdes;
import org.geovistory.toolbox.streams.base.model.BuilderSingleton;
import org.geovistory.toolbox.streams.base.model.InputTopicNames;
import org.geovistory.toolbox.streams.base.model.OutputTopicNames;
import org.geovistory.toolbox.streams.lib.ProjectedTableRegistrar;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped

public class OntomeClassProjected {

    @Inject
    AvroSerdes avroSerdes;
    @Inject
    BuilderSingleton builderSingleton;
    @Inject
    InputTopicNames inputTopicNames;
    @Inject
    OutputTopicNames outputTopicNames;

    private ProjectedTableRegistrar<
            dev.data_for_history.api_class.Key,
            dev.data_for_history.api_class.Value,
            OntomeClassKey,
            OntomeClassValue
            > registrar;

    public ProjectedTableRegistrar<
            dev.data_for_history.api_class.Key,
            dev.data_for_history.api_class.Value,
            OntomeClassKey,
            OntomeClassValue
            > getRegistrar(AvroSerdes avroSerdes, BuilderSingleton builderSingleton, InputTopicNames inputTopicNames, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.builderSingleton = builderSingleton;
        this.inputTopicNames = inputTopicNames;
        this.outputTopicNames = outputTopicNames;
        return getRegistrar();
    }

    public ProjectedTableRegistrar<
            dev.data_for_history.api_class.Key,
            dev.data_for_history.api_class.Value,
            OntomeClassKey,
            OntomeClassValue
            > getRegistrar() {
        if (registrar == null) {
            registrar = new ProjectedTableRegistrar<>(
                    builderSingleton.builder,
                    // input topic name
                    inputTopicNames.dfhApiClass(),
                    // input key serde
                    avroSerdes.DfhApiClassKey(),
                    // input value serde
                    avroSerdes.DfhApiClassValue(),
                    // prefix for outputs
                    outputTopicNames.ontomeClass(),
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
        return registrar;
    }


}
