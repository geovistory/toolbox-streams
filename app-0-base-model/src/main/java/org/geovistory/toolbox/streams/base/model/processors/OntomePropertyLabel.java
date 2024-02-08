package org.geovistory.toolbox.streams.base.model.processors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.OntomePropertyKey;
import org.geovistory.toolbox.streams.avro.OntomePropertyLabelKey;
import org.geovistory.toolbox.streams.avro.OntomePropertyLabelValue;
import org.geovistory.toolbox.streams.avro.OntomePropertyValue;
import org.geovistory.toolbox.streams.base.model.BuilderSingleton;
import org.geovistory.toolbox.streams.base.model.InputTopicNames;
import org.geovistory.toolbox.streams.base.model.OutputTopicNames;
import org.geovistory.toolbox.streams.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.lib.IdenticalRecordsFilterSupplier;
import org.geovistory.toolbox.streams.lib.TopicNameEnum;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.LinkedList;
import java.util.List;

@ApplicationScoped
public class OntomePropertyLabel {
    @Inject
    ConfiguredAvroSerde as;

    @ConfigProperty(name = "ts.input.topic.name.prefix", defaultValue = "")
    String inPrefix;

    @Inject
    BuilderSingleton builderSingleton;

    @Inject
    InputTopicNames inputTopicNames;

    @Inject
    OutputTopicNames outputTopicNames;

    public OntomePropertyLabel(ConfiguredAvroSerde as, BuilderSingleton builderSingleton, InputTopicNames inputTopicNames, OutputTopicNames outputTopicNames) {
        this.as = as;
        this.builderSingleton = builderSingleton;
        this.inputTopicNames = inputTopicNames;
        this.outputTopicNames = outputTopicNames;
    }


    public void addProcessorsStandalone() {
        addProcessors(
                new OntomePropertyProjected().getRegistrar(
                        this.as, this.builderSingleton, this.inputTopicNames, this.outputTopicNames
                ).kStream
        );
    }


    public OntomePropertyLabelReturnValue addProcessors(
            KStream<OntomePropertyKey, OntomePropertyValue> ontomePropertyStream
    ) {

        /* STREAM PROCESSORS */
        // 2)
        var ontomePropertyLabel = ontomePropertyStream
                .flatMap(
                        (key, value) -> {
                            List<KeyValue<OntomePropertyLabelKey, OntomePropertyLabelValue>> result = new LinkedList<>();

                            var langId = Utils.isoLangToGeoId(value.getDfhPropertyLabelLanguage());
                            if (langId == null) return result;
                            var k = OntomePropertyLabelKey.newBuilder()
                                    .setPropertyId(value.getDfhPkProperty())
                                    .setLanguageId(langId)
                                    .build();
                            var v = OntomePropertyLabelValue.newBuilder()
                                    .setPropertyId(value.getDfhPkProperty())
                                    .setLanguageId(langId)
                                    .setLabel(value.getDfhPropertyLabel())
                                    .setInverseLabel(value.getDfhPropertyInverseLabel())
                                    .build();
                            result.add(KeyValue.pair(k, v));
                            return result;
                        },
                        Named.as("kstream-flatmap-ontome-property-to-ontome-property-label")
                )
                .transform(new IdenticalRecordsFilterSupplier<>(
                                "ontome_property_label_suppress_duplicates",
                                as.key(),
                                as.value()),
                        Named.as("ontome_property_label_suppress_duplicates"));

        /* SINK PROCESSORS */
        ontomePropertyLabel
                .to(
                        outputTopicNames.ontomePropertyLabel(),
                        Produced.with(as.<OntomePropertyLabelKey>key(), as.<OntomePropertyLabelValue>value())
                                .withName(outputTopicNames.ontomePropertyLabel() + "-producer")
                );


        return new OntomePropertyLabelReturnValue(ontomePropertyLabel);

    }


    public String inDfhApiProperty() {
        return Utils.prefixedIn(inPrefix, TopicNameEnum.dfh_api_property.getValue());
    }


}
