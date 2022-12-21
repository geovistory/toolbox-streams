package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.avro.OntomeClassLabelKey;
import org.geovistory.toolbox.streams.avro.OntomeClassLabelValue;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.LinkedList;
import java.util.List;


public class OntomeClassLabel {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {

        return addProcessors(builder).builder().build();
    }

    public static OntomeClassLabelReturnValue addProcessors(StreamsBuilder builder) {

        var avroSerdes = new ConfluentAvroSerdes();

        /* SOURCE PROCESSORS */

        // 1) register api_class
        var ontomeClassStream = builder
                .stream(input.TOPICS.api_class,
                        Consumed.with(avroSerdes.DfhApiClassKey(), avroSerdes.DfhApiClassValue()));


        /* STREAM PROCESSORS */
        // 2)
        var ontomeClassLabel = ontomeClassStream
                .flatMap((key, value) -> {
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
                });

        /* SINK PROCESSORS */
        ontomeClassLabel
                .to(
                        output.TOPICS.ontome_class_label,
                        Produced.with(avroSerdes.OntomeClassLabelKey(), avroSerdes.OntomeClassLabelValue())
                );


        return new OntomeClassLabelReturnValue(builder, ontomeClassLabel);

    }


    public enum input {
        TOPICS;
        public final String api_class = Utils.dbPrefixed("data_for_history.api_class");


    }


    public enum output {
        TOPICS;
        public final String ontome_class_label = Utils.tsPrefixed("ontome_class_label");

    }

}
