package org.geovistory.toolbox.streams.project.config.processors;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.project.config.*;


public class CommunityClassLabel {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerInputTopic = new RegisterInputTopic(builder);
        var registerInnerTopic = new RegisterInnerTopic(builder);

        return addProcessors(
                builder,
                registerInputTopic.ontomeClassLabelTable(),
                registerInnerTopic.geovClassLabelStream()
        ).builder().build();
    }

    public static CommunityClassLabelReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<OntomeClassLabelKey, OntomeClassLabelValue> ontomeClassLabelTable,
            KStream<GeovClassLabelKey, GeovClassLabelValue> geovClassLabelStream
    ) {

        var avroSerdes = new ConfluentAvroSerdes();

        var rekeyedStream = geovClassLabelStream
                .filter(
                        (key, value) -> key.getProjectId() == I.DEFAULT_PROJECT.get(),
                        Named.as("kstream-geov-class-label-filter-default-project")
                )
                .selectKey(
                        (key, value) -> OntomeClassLabelKey.newBuilder()
                                .setClassId(value.getClassId())
                                .setLanguageId(value.getLanguageId())
                                .build(),
                        Named.as("kstream-select-key-geov-class-label-default-project")
                );
        var defaultGeovClassLabels = rekeyedStream.toTable(
                Named.as(inner.TOPICS.default_geov_class_label_by_ontome_class_label_key),
                Materialized.<OntomeClassLabelKey, GeovClassLabelValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.default_geov_class_label_by_ontome_class_label_key)
                        .withKeySerde(avroSerdes.OntomeClassLabelKey())
                        .withValueSerde(avroSerdes.GeovClassLabelValue())
        );
        // 1
        var communityClassLabelTable = defaultGeovClassLabels.outerJoin(
                ontomeClassLabelTable,
                (value1, value2) -> CommunityClassLabelValue.newBuilder()
                        .setLabel((value1 != null && value1.getLabel() != null) ? value1.getLabel() :
                                value2 != null ? value2.getLabel() : "")
                        .setDeleted$1(
                                (value1 != null && Utils.booleanIsEqualTrue(value1.getDeleted$1()))
                                        && (value2 != null && Utils.booleanIsEqualTrue(value2.getDeleted$1())))
                        .build(),
                Named.as(inner.TOPICS.community_class_label + "-outer-join"),
                Materialized.<OntomeClassLabelKey, CommunityClassLabelValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.community_class_label)
                        .withKeySerde(avroSerdes.OntomeClassLabelKey())
                        .withValueSerde(avroSerdes.CommunityClassLabelValue())
        );

        var communityClassLabelStream = communityClassLabelTable.toStream(Named.as("ktable-to-stream-community_class_label"));
        communityClassLabelStream.to(
                output.TOPICS.community_class_label,
                Produced.with(avroSerdes.OntomeClassLabelKey(), avroSerdes.CommunityClassLabelValue())
                        .withName(output.TOPICS.community_class_label + "-producer")
        );
        return new CommunityClassLabelReturnValue(builder, communityClassLabelTable, communityClassLabelStream);

    }


    public enum input {
        TOPICS;
        public final String ontome_class_label = Env.INSTANCE.TOPIC_ONTOME_CLASS_LABEL;
        public final String geov_class_label = GeovClassLabel.output.TOPICS.geov_class_label;
        public final String project_class = ProjectClass.output.TOPICS.project_class;

        public final String project = DbTopicNames.pro_projects.getName();
    }


    public enum inner {
        TOPICS;
        public final String community_class_label = "community_class_label";
        public final String default_geov_class_label_by_ontome_class_label_key = "default_geov_class_label_by_ontome_class_label_key";

    }

    public enum output {
        TOPICS;
        public final String community_class_label = Utils.tsPrefixed("community_class_label");
    }

}
