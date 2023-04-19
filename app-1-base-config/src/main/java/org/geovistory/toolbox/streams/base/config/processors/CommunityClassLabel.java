package org.geovistory.toolbox.streams.base.config.processors;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.base.config.*;
import org.geovistory.toolbox.streams.lib.IdenticalRecordsFilterSupplier;
import org.geovistory.toolbox.streams.lib.Utils;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;


@ApplicationScoped
public class CommunityClassLabel {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;
    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public CommunityClassLabel(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }
    public void addProcessorsStandalone() {

        addProcessors(
                registerInputTopic.ontomeClassLabelTable(),
                registerInnerTopic.geovClassLabelStream()
        );
    }

    public CommunityClassLabelReturnValue addProcessors(
            KTable<OntomeClassLabelKey, OntomeClassLabelValue> ontomeClassLabelTable,
            KStream<GeovClassLabelKey, GeovClassLabelValue> geovClassLabelStream
    ) {

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

        var communityClassLabelStream = communityClassLabelTable
                .toStream(Named.as("ktable-to-stream-community_class_label"))
                .transform(new IdenticalRecordsFilterSupplier<>(
                        "community_class_label_identical_records_filter",
                        avroSerdes.OntomeClassLabelKey(),
                        avroSerdes.CommunityClassLabelValue()
                ));

        communityClassLabelStream.to(
                outputTopicNames.communityClassLabel(),
                Produced.with(avroSerdes.OntomeClassLabelKey(), avroSerdes.CommunityClassLabelValue())
                        .withName(outputTopicNames.communityClassLabel() + "-producer")
        );
        return new CommunityClassLabelReturnValue(communityClassLabelTable, communityClassLabelStream);

    }

    public enum inner {
        TOPICS;
        public final String community_class_label = "community_class_label";
        public final String default_geov_class_label_by_ontome_class_label_key = "default_geov_class_label_by_ontome_class_label_key";

    }


}
