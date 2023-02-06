package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.app.DbTopicNames;
import org.geovistory.toolbox.streams.avro.OntomePropertyKey;
import org.geovistory.toolbox.streams.avro.OntomePropertyLabelKey;
import org.geovistory.toolbox.streams.avro.OntomePropertyLabelValue;
import org.geovistory.toolbox.streams.avro.OntomePropertyValue;
import org.geovistory.toolbox.streams.input.OntomePropertyProjected;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.LinkedList;
import java.util.List;


public class OntomePropertyLabel {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {



        return addProcessors(builder, new OntomePropertyProjected(builder).kStream).builder().build();

    }

    public static OntomePropertyLabelReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<OntomePropertyKey, OntomePropertyValue> ontomePropertyStream
    ) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)
        var ontomePropertyLabel = ontomePropertyStream
                .flatMap((key, value) -> {
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
                });

        /* SINK PROCESSORS */
        ontomePropertyLabel
                .to(
                        output.TOPICS.ontome_property_label,
                        Produced.with(avroSerdes.OntomePropertyLabelKey(), avroSerdes.OntomePropertyLabelValue())
                );


        return new OntomePropertyLabelReturnValue(builder, ontomePropertyLabel);

    }


    public enum input {
        TOPICS;
        public final String api_property = DbTopicNames.dfh_api_property.getName();


    }


    public enum output {
        TOPICS;
        public final String ontome_property_label = Utils.tsPrefixed("ontome_property_label");

    }

}
