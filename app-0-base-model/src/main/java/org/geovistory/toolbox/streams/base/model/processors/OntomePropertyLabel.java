package org.geovistory.toolbox.streams.base.model.processors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.OntomePropertyKey;
import org.geovistory.toolbox.streams.avro.OntomePropertyLabelKey;
import org.geovistory.toolbox.streams.avro.OntomePropertyLabelValue;
import org.geovistory.toolbox.streams.avro.OntomePropertyValue;
import org.geovistory.toolbox.streams.base.model.AvroSerdes;
import org.geovistory.toolbox.streams.base.model.BuilderSingleton;
import org.geovistory.toolbox.streams.lib.IdenticalRecordsFilterSupplier;
import org.geovistory.toolbox.streams.lib.TopicNameEnum;
import org.geovistory.toolbox.streams.lib.Utils;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

@ApplicationScoped
public class OntomePropertyLabel {
    AvroSerdes avroSerdes;

    @ConfigProperty(name = "ts.input.topic.name.prefix", defaultValue = "")
    String inPrefix;
    @ConfigProperty(name = "ts.output.topic.name.prefix", defaultValue = "")
    public String outPrefix;

    @Inject
    BuilderSingleton builderSingleton;

    public OntomePropertyLabel(AvroSerdes avroSerdes, BuilderSingleton builderSingleton) {
        this.avroSerdes = avroSerdes;
        this.builderSingleton = builderSingleton;
    }

    public void addProcessorsStandalone() {

        var o = new OntomePropertyProjected(
                avroSerdes,
                builderSingleton.builder,
                inDfhApiProperty(),
                outOntomePropertyLabel()
        );
        addProcessors(o.kStream);
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
                                avroSerdes.OntomePropertyLabelKey(),
                                avroSerdes.OntomePropertyLabelValue()),
                        Named.as("ontome_property_label_suppress_duplicates"));

        /* SINK PROCESSORS */
        ontomePropertyLabel
                .to(
                        outOntomePropertyLabel(),
                        Produced.with(avroSerdes.OntomePropertyLabelKey(), avroSerdes.OntomePropertyLabelValue())
                                .withName(outOntomePropertyLabel() + "-producer")
                );


        return new OntomePropertyLabelReturnValue(ontomePropertyLabel);

    }


    public String inDfhApiProperty() {
        return Utils.prefixedIn(inPrefix, TopicNameEnum.dfh_api_property.getValue());
    }


    public String outOntomePropertyLabel() {
        return Utils.prefixedOut(outPrefix, "ontome_property_label");
    }


}
