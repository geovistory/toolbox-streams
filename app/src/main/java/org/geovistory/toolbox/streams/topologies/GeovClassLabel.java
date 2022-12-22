package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.geovistory.toolbox.streams.app.DbTopicNames;
import org.geovistory.toolbox.streams.app.RegisterInputTopic;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;


public class GeovClassLabel {

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

    public static GeovClassLabelReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<dev.projects.text_property.Key, dev.projects.text_property.Value> proTextPropertyTable
    ) {

        var avroSerdes = new ConfluentAvroSerdes();

        /* SOURCE PROCESSORS */

        // 1) register text_property
        var textPropertyStream = proTextPropertyTable.toStream();

        /* STREAM PROCESSORS */

        // 2)
        var geovClassLabel = textPropertyStream
                .flatMap((key, value) -> {
                    List<KeyValue<GeovClassLabelKey, GeovClassLabelValue>> result = new LinkedList<>();
                    var classId = value.getFkDfhClass();

                    // validate
                    if (classId == null) return result;
                    if (value.getFkSystemType() != 639) return result;

                    var k = GeovClassLabelKey.newBuilder()
                            .setProjectId(value.getFkProject())
                            .setClassId(value.getFkDfhClass())
                            .setLanguageId(value.getFkLanguage())
                            .build();
                    var v = GeovClassLabelValue.newBuilder()
                            .setProjectId(value.getFkProject())
                            .setClassId(value.getFkDfhClass())
                            .setLanguageId(value.getFkLanguage())
                            .setLabel(value.getString())
                            .setDeleted$1(Objects.equals(value.getDeleted$1(), "true"))
                            .build();

                    result.add(KeyValue.pair(k, v));
                    return result;
                });

        /* SINK PROCESSORS */
        geovClassLabel
                .to(
                        output.TOPICS.geov_class_label,
                        Produced.with(avroSerdes.GeovClassLabelKey(), avroSerdes.GeovClassLabelValue())
                );

        return new GeovClassLabelReturnValue(builder, geovClassLabel);

    }


    public enum input {
        TOPICS;
        public final String text_property = DbTopicNames.pro_text_property.getName();

    }

    public enum output {
        TOPICS;
        public final String geov_class_label = Utils.tsPrefixed("geov_class_label");

    }

}
