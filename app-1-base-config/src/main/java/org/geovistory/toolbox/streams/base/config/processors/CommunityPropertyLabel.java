package org.geovistory.toolbox.streams.base.config.processors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.base.config.I;
import org.geovistory.toolbox.streams.base.config.OutputTopicNames;
import org.geovistory.toolbox.streams.base.config.RegisterInnerTopic;
import org.geovistory.toolbox.streams.base.config.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.lib.IdenticalRecordsFilterSupplier;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.LinkedList;


@ApplicationScoped
public class CommunityPropertyLabel {
    @Inject
    ConfiguredAvroSerde as;

    @Inject
    RegisterInputTopic registerInputTopic;

    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public CommunityPropertyLabel(ConfiguredAvroSerde as, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.as = as;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
        addProcessors(
                registerInputTopic.ontomePropertyStream(),
                registerInnerTopic.geovPropertyLabelStream()
        );
    }

    public CommunityPropertyLabelReturnValue addProcessors(
            KStream<OntomePropertyKey, OntomePropertyValue> ontomePropertyStream,
            KStream<GeovPropertyLabelKey, GeovPropertyLabelValue> geovPropertyLabelStream
    ) {

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
                                    .setPropertyId(value.getDfhPkProperty())
                                    .setIsOutgoing(true)
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
                                    .setPropertyId(value.getDfhPkProperty())
                                    .setIsOutgoing(false)
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
                        .withKeySerde(as.key())
                        .withValueSerde(as.value())
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
                        .withKeySerde(as.key())
                        .withValueSerde(as.value())
        );
        // 5
        var communityPropertyLabelTable = defaultGeovPropertyLabelTable.outerJoin(
                langFieldsTable,
                (value1, value2) -> CommunityPropertyLabelValue.newBuilder()
                        .setLabel((value1 != null && value1.getLabel() != null) ? value1.getLabel() :
                                value2 != null ? value2.getLabel() : "")
                        .setPropertyId((value1 != null ? value1.getPropertyId() : value2 != null ? value2.getPropertyId() : 0))
                        .setIsOutgoing(value1 != null ? value1.getIsOutgoing() : value2 == null || value2.getIsOutgoing())
                        .setDeleted$1(
                                (value1 != null && Utils.booleanIsEqualTrue(value1.getDeleted$1()))
                                        && (value2 != null && Utils.booleanIsEqualTrue(value2.getDeleted$1())))
                        .build(),
                Named.as(inner.TOPICS.community_property_label + "-outer-join"),
                Materialized.<CommunityPropertyLabelKey, CommunityPropertyLabelValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.community_property_label)
                        .withKeySerde(as.key())
                        .withValueSerde(as.value())
        );

        var communityPropertyLabelStream = communityPropertyLabelTable
                .toStream(Named.as("ktable-to-stream-community_property_label"))
                .transform(new IdenticalRecordsFilterSupplier<>(
                        "community_community_property_label_label_identical_records_filter",
                        as.key(),
                        as.value()
                ));

        communityPropertyLabelStream.to(
                outputTopicNames.communityPropertyLabel(),
                Produced.with(as.<CommunityPropertyLabelKey>key(), as.<CommunityPropertyLabelValue>value())
                        .withName(outputTopicNames.communityPropertyLabel() + "-producer")
        );
        return new CommunityPropertyLabelReturnValue(communityPropertyLabelTable, communityPropertyLabelStream);

    }

    public enum inner {
        TOPICS;
        public final String community_property_label = "community_property_label";
        public final String default_geov_property_label_by_ontome_property_label_key = "ontome_property_label_by_geov_property_label_key";

        public final String ontome_property_by_lang_and_direction = "ontome_property_by_lang_and_direction";
    }


}
