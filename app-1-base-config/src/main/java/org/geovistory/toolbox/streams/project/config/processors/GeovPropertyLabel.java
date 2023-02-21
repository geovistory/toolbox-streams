package org.geovistory.toolbox.streams.project.config.processors;

import dev.projects.text_property.Key;
import dev.projects.text_property.Value;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.avro.GeovPropertyLabelKey;
import org.geovistory.toolbox.streams.avro.GeovPropertyLabelValue;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.project.config.DbTopicNames;
import org.geovistory.toolbox.streams.project.config.RegisterInputTopic;

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
                register.proTextPropertyStream()
        ).builder().build();
    }

    public static GeovPropertyLabelReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<Key, Value> proTextPropertyStream
    ) {

        var avroSerdes = new ConfluentAvroSerdes();

        /* SOURCE PROCESSORS */

        /* STREAM PROCESSORS */

        // 2)
        var geovPropertyLabel = proTextPropertyStream
                .flatMap(
                        (key, value) -> {
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
                        },
                        Named.as("kstream-flatmap-pro-text-property-to-geov-property-label")
                );

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
