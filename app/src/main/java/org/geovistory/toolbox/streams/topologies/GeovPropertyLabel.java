package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.app.DbTopicNames;
import org.geovistory.toolbox.streams.app.RegisterInputTopic;
import org.geovistory.toolbox.streams.avro.GeovPropertyLabelKey;
import org.geovistory.toolbox.streams.avro.GeovPropertyLabelValue;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;


public class GeovPropertyLabel {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var register = new RegisterInputTopic(builder);

        return addProcessors(
                builder,
                register.proTextPropertyTable()
        ).builder().build();
    }

    public static GeovPropertyLabelReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<dev.projects.text_property.Key, dev.projects.text_property.Value> proTextPropertyTable
    ) {

        var avroSerdes = new ConfluentAvroSerdes();

        /* SOURCE PROCESSORS */

        // 1) register text_property
        var textPropertyStream = proTextPropertyTable.toStream();

        /* STREAM PROCESSORS */

        // 2)
        var geovPropertyLabel = textPropertyStream
                .flatMap((key, value) -> {
                    List<KeyValue<GeovPropertyLabelKey, GeovPropertyLabelValue>> result = new LinkedList<>();
                    var propertyId = value.getFkDfhProperty();
                    var domainId = value.getFkDfhPropertyDomain();
                    var rangeId = value.getFkDfhPropertyRange();

                    // validate
                    if (propertyId == null) return result;
                    if (value.getFkDfhProperty() == null) return result;
                    if (domainId == null && rangeId == null) return result;

                    int classId = domainId != null ? domainId : rangeId;
                    var isOutgoing = domainId != null;

                    var k = GeovPropertyLabelKey.newBuilder()
                            .setProjectId(value.getFkProject())
                            .setClassId(classId)
                            .setIsOutgoing(isOutgoing)
                            .setPropertyId(value.getFkDfhProperty())
                            .setLanguageId(value.getFkLanguage())
                            .build();
                    var v = GeovPropertyLabelValue.newBuilder()
                            .setProjectId(value.getFkProject())
                            .setProjectId(value.getFkProject())
                            .setClassId(classId)
                            .setIsOutgoing(isOutgoing)
                            .setPropertyId(value.getFkDfhProperty())
                            .setLanguageId(value.getFkLanguage())
                            .setLabel(value.getString())
                            .setDeleted$1(Objects.equals(value.getDeleted$1(), "true"))
                            .build();

                    result.add(KeyValue.pair(k, v));
                    return result;
                });

        /* SINK PROCESSORS */
        geovPropertyLabel
                .to(
                        output.TOPICS.geov_property_label,
                        Produced.with(avroSerdes.GeovPropertyLabelKey(), avroSerdes.GeovPropertyLabelValue())
                );

        return new GeovPropertyLabelReturnValue(builder, geovPropertyLabel);

    }


    public enum input {
        TOPICS;
        public final String text_property = DbTopicNames.pro_text_property.getName();

    }

    public enum output {
        TOPICS;
        public final String geov_property_label = Utils.tsPrefixed("geov_property_label");

    }

}
