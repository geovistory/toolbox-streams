package org.geovistory.toolbox.streams.base.model.processors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.OntomeClassKey;
import org.geovistory.toolbox.streams.avro.OntomeClassLabelKey;
import org.geovistory.toolbox.streams.avro.OntomeClassLabelValue;
import org.geovistory.toolbox.streams.avro.OntomeClassValue;
import org.geovistory.toolbox.streams.base.model.AvroSerdes;
import org.geovistory.toolbox.streams.base.model.BuilderSingleton;
import org.geovistory.toolbox.streams.base.model.InputTopicNames;
import org.geovistory.toolbox.streams.base.model.OutputTopicNames;
import org.geovistory.toolbox.streams.lib.IdenticalRecordsFilterSupplier;
import org.geovistory.toolbox.streams.lib.TopicNameEnum;
import org.geovistory.toolbox.streams.lib.Utils;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedList;
import java.util.List;

@ApplicationScoped
public class OntomeClassLabel {

    AvroSerdes avroSerdes;

    @ConfigProperty(name = "ts.input.topic.name.prefix", defaultValue = "")
    String inPrefix;
    @Inject
    BuilderSingleton builderSingleton;

    @Inject
    InputTopicNames inputTopicNames;

    @Inject
    OutputTopicNames outputTopicNames;

    public OntomeClassLabel(AvroSerdes avroSerdes, BuilderSingleton builderSingleton, InputTopicNames inputTopicNames, OutputTopicNames outputTopicNames) {
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

    public OntomeClassLabelReturnValue addProcessors(
            KStream<OntomeClassKey, OntomeClassValue> ontomeClassStream
    ) {

        /* STREAM PROCESSORS */
        // 2)
        var ontomeClassLabel = ontomeClassStream
                .flatMap(
                        (key, value) -> {
                            List<KeyValue<OntomeClassLabelKey, OntomeClassLabelValue>> result = new LinkedList<>();

                            var langId = Utils.isoLangToGeoId(value.getDfhClassLabelLanguage());
                            if (langId == null) return result;
                            var k = OntomeClassLabelKey.newBuilder()
                                    .setClassId(value.getDfhPkClass())
                                    .setLanguageId(langId)
                                    .build();
                            var v = OntomeClassLabelValue.newBuilder()
                                    .setClassId(value.getDfhPkClass())
                                    .setLanguageId(langId)
                                    .setLabel(value.getDfhClassLabel())
                                    //  .setDeleted$1(Objects.equals(value.getDeleted$1(), "true"))
                                    .build();
                            result.add(KeyValue.pair(k, v));
                            return result;
                        },
                        Named.as("kstream-flatmap-ontome-class-to-ontome-class-label")
                )
                .transform(new IdenticalRecordsFilterSupplier<>(
                                "ontome_class_label_suppress_duplicates",
                                avroSerdes.OntomeClassLabelKey(),
                                avroSerdes.OntomeClassLabelValue()),
                        Named.as("ontome_class_label_suppress_duplicates"));

        /* SINK PROCESSORS */
        ontomeClassLabel
                .to(
                        outputTopicNames.ontomeClassLabel(),
                        Produced.with(avroSerdes.OntomeClassLabelKey(), avroSerdes.OntomeClassLabelValue())
                                .withName(outputTopicNames.ontomeClassLabel() + "-producer")
                );


        return new OntomeClassLabelReturnValue(ontomeClassLabel);

    }


    public String inApiClass() {
        return Utils.prefixedIn(inPrefix, TopicNameEnum.dfh_api_class.getValue());
    }



}
