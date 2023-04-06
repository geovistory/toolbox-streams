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
import org.geovistory.toolbox.streams.lib.TopicNameEnum;
import org.geovistory.toolbox.streams.lib.Utils;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

@ApplicationScoped
public class OntomeClassLabel {

    AvroSerdes avroSerdes;

    @ConfigProperty(name = "ts.input.topic.name.prefix", defaultValue = "")
    String inPrefix;
    @ConfigProperty(name = "ts.output.topic.name.prefix", defaultValue = "")
    public String outPrefix;

    @Inject
    BuilderSingleton builderSingleton;

    public OntomeClassLabel(AvroSerdes avroSerdes, BuilderSingleton builderSingleton) {
        this.avroSerdes = avroSerdes;
        this.builderSingleton = builderSingleton;
    }

    public void addProcessorsStandalone() {
        var o = new OntomeClassProjected(
                avroSerdes,
                builderSingleton.builder,
                inApiClass(),
                outOntomeClassLabel()
        );
        addProcessors(o.kStream);
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
                );

        /* SINK PROCESSORS */
        ontomeClassLabel
                .to(
                        outOntomeClassLabel(),
                        Produced.with(avroSerdes.OntomeClassLabelKey(), avroSerdes.OntomeClassLabelValue())
                                .withName(outOntomeClassLabel() + "-producer")
                );


        return new OntomeClassLabelReturnValue(ontomeClassLabel);

    }




    public String inApiClass() {
        return Utils.prefixedIn(inPrefix, TopicNameEnum.dfh_api_class.getValue());
    }


    public String outOntomeClassLabel() {
        return Utils.prefixedOut(outPrefix, "ontome_class_label");
    }


}
