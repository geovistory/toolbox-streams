package org.geovistory.toolbox.streams.project.config.processors;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.project.config.*;

import java.util.LinkedList;


public class CommunityPropertyLabel {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerInputTopic = new RegisterInputTopic(builder);
        var registerInnerTopic = new RegisterInnerTopic(builder);

        return addProcessors(
                builder,
                registerInputTopic.ontomePropertyStream(),
                registerInnerTopic.geovPropertyLabelStream()
        ).builder().build();
    }

    public static CommunityPropertyLabelReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<OntomePropertyKey, OntomePropertyValue> ontomePropertyStream,
            KStream<GeovPropertyLabelKey, GeovPropertyLabelValue> geovPropertyLabelStream
    ) {

        var avroSerdes = new ConfluentAvroSerdes();

        // 2
        var langFieldsStream = ontomePropertyStream.flatMap(
                (key, value) -> {
                    var result = new LinkedList<KeyValue<CommunityPropertyLabelKey, CommunityPropertyLabelValue>>();
                    var langid = Utils.isoLangToGeoId(value.getDfhPropertyLabelLanguage());
                    result.add(KeyValue.pair(
                            CommunityPropertyLabelKey.newBuilder()
                                    .setPropertyId(value.getDfhPkProperty())
                                    .setClassId(value.getDfhPropertyDomain())
                                    .setIsOutgoing(true)
                                    .setLanguageId(langid)
                                    .build(),
                            CommunityPropertyLabelValue.newBuilder()
                                    .setLabel(value.getDfhPropertyLabel() != null ? value.getDfhPropertyLabel() : "")
                                    .setDeleted$1(Utils.stringIsEqualTrue(value.getDeleted$1()))
                                    .build()
                    ));
                    result.add(KeyValue.pair(
                            CommunityPropertyLabelKey.newBuilder()
                                    .setPropertyId(value.getDfhPkProperty())
                                    .setClassId(value.getDfhPropertyRange())
                                    .setIsOutgoing(false)
                                    .setLanguageId(langid)
                                    .build(),
                            CommunityPropertyLabelValue.newBuilder()
                                    .setLabel(value.getDfhPropertyInverseLabel() != null ? value.getDfhPropertyInverseLabel() : "")
                                    .setDeleted$1(Utils.stringIsEqualTrue(value.getDeleted$1()))
                                    .build()
                    ));
                    return result;
                },
                Named.as("kstream-flat-map-ontome-property-to-lang-fields")
        );

        // 3
        var langFieldsTable = langFieldsStream.toTable(
                Named.as(inner.TOPICS.ontome_property_by_lang_and_direction),
                Materialized.<CommunityPropertyLabelKey, CommunityPropertyLabelValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.ontome_property_by_lang_and_direction)
                        .withKeySerde(avroSerdes.CommunityPropertyLabelKey())
                        .withValueSerde(avroSerdes.CommunityPropertyLabelValue())
        );
        // 4
        var defaultGeovPropertyLabelStream = geovPropertyLabelStream
                .filter(
                        (key, value) -> key.getProjectId() == I.DEFAULT_PROJECT.get(),
                        Named.as("kstream-geov-property-label-filter-default-project")
                )
                .selectKey(
                        (key, value) -> CommunityPropertyLabelKey.newBuilder()
                                .setClassId(key.getClassId())
                                .setPropertyId(value.getPropertyId())
                                .setIsOutgoing(key.getIsOutgoing())
                                .setLanguageId(value.getLanguageId())
                                .build(),
                        Named.as("kstream-select-key-geov-property-label-default-project")
                );
        var defaultGeovPropertyLabelTable = defaultGeovPropertyLabelStream.toTable(
                Named.as(inner.TOPICS.default_geov_property_label_by_ontome_property_label_key),
                Materialized.<CommunityPropertyLabelKey, GeovPropertyLabelValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.default_geov_property_label_by_ontome_property_label_key)
                        .withKeySerde(avroSerdes.CommunityPropertyLabelKey())
                        .withValueSerde(avroSerdes.GeovPropertyLabelValue())
        );
        // 5
        var communityPropertyLabelTable = defaultGeovPropertyLabelTable.outerJoin(
                langFieldsTable,
                (value1, value2) -> CommunityPropertyLabelValue.newBuilder()
                        .setLabel((value1 != null && value1.getLabel() != null) ? value1.getLabel() :
                                value2 != null ? value2.getLabel() : "")
                        .setDeleted$1(
                                (value1 != null && Utils.booleanIsEqualTrue(value1.getDeleted$1()))
                                        && (value2 != null && Utils.booleanIsEqualTrue(value2.getDeleted$1())))
                        .build(),
                Named.as(inner.TOPICS.community_property_label + "-outer-join"),
                Materialized.<CommunityPropertyLabelKey, CommunityPropertyLabelValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.community_property_label)
                        .withKeySerde(avroSerdes.CommunityPropertyLabelKey())
                        .withValueSerde(avroSerdes.CommunityPropertyLabelValue())
        );

        var communityPropertyLabelStream = communityPropertyLabelTable.toStream(Named.as("ktable-to-stream-community_property_label"));
        communityPropertyLabelStream.to(
                output.TOPICS.community_property_label,
                Produced.with(avroSerdes.CommunityPropertyLabelKey(), avroSerdes.CommunityPropertyLabelValue())
                        .withName(output.TOPICS.community_property_label + "-producer")
        );
        return new CommunityPropertyLabelReturnValue(builder, communityPropertyLabelTable, communityPropertyLabelStream);

    }

    public enum input {
        TOPICS;
        public final String geov_property_label = GeovPropertyLabel.output.TOPICS.geov_property_label;
        public final String ontome_property = Env.INSTANCE.TOPIC_ONTOME_PROPERTY;

        public final String project = DbTopicNames.pro_projects.getName();
    }


    public enum inner {
        TOPICS;
        public final String community_property_label = "community_property_label";
        public final String default_geov_property_label_by_ontome_property_label_key = "ontome_property_label_by_geov_property_label_key";

        public final String ontome_property_by_lang_and_direction = "ontome_property_by_lang_and_direction";
    }

    public enum output {
        TOPICS;
        public final String community_property_label = Utils.tsPrefixed("community_property_label");
    }

}
