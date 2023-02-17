package org.geovistory.toolbox.streams.base.model.processors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.avro.OntomeClassKey;
import org.geovistory.toolbox.streams.avro.OntomeClassLabelKey;
import org.geovistory.toolbox.streams.avro.OntomeClassLabelValue;
import org.geovistory.toolbox.streams.avro.OntomeClassValue;
import org.geovistory.toolbox.streams.base.model.DbTopicNames;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.LinkedList;
import java.util.List;


public class OntomeClassLabel {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {

        var ontomeClassStream = new OntomeClassProjected(builder).kStream;

        return addProcessors(builder, ontomeClassStream).builder().build();

    }

    public static OntomeClassLabelReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<OntomeClassKey, OntomeClassValue> ontomeClassStream
    ) {

        var avroSerdes = new ConfluentAvroSerdes();

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
                        output.TOPICS.ontome_class_label,
                        Produced.with(avroSerdes.OntomeClassLabelKey(), avroSerdes.OntomeClassLabelValue())
                                .withName(output.TOPICS.ontome_class_label + "-producer")
                );


        return new OntomeClassLabelReturnValue(builder, ontomeClassLabel);

    }


    public enum input {
        TOPICS;
        public final String api_class = DbTopicNames.dfh_api_class.getName();
    }


    public enum output {
        TOPICS;
        public final String ontome_class_label = Utils.tsPrefixed("ontome_class_label");

    }

}
