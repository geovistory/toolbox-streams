package org.geovistory.toolbox.streams.fulltext.processors.community;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.fulltext.AvroSerdes;
import org.geovistory.toolbox.streams.fulltext.I;
import org.geovistory.toolbox.streams.fulltext.OutputTopicNames;
import org.geovistory.toolbox.streams.fulltext.RegisterInputTopic;
import org.geovistory.toolbox.streams.fulltext.processors.FullTextFactory;
import org.geovistory.toolbox.streams.lib.Utils;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.LinkedList;


@ApplicationScoped
public class CommunityEntityFulltext {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;


    @Inject
    OutputTopicNames outputTopicNames;

    @ConfigProperty(name = "ts.community.slug", defaultValue = "")
    private String communitySlug;


    public CommunityEntityFulltext(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {

        addProcessors(
                registerInputTopic.communityEntityWithLabelConfigTable(),
                registerInputTopic.communityTopStatementsTable(),
                registerInputTopic.communityPropertyLabelTable()
        );

    }

    public void addProcessors(
            KTable<CommunityEntityKey, CommunityEntityLabelConfigValue> communityEntityWithLabelConfigTable,
            KTable<CommunityTopStatementsKey, CommunityTopStatementsValue> communityTopStatementsTable,
            KTable<CommunityPropertyLabelKey, CommunityPropertyLabelValue> communityPropertyLabelTable
    ) {

        /* STREAM PROCESSORS */
        // 2
        var n2 = "community_" + communitySlug + "field_top_labels_store";
        var communityFieldTopLabelsTable = communityTopStatementsTable.mapValues((readOnlyKey, value) -> {
                    var l = new LinkedList<String>();
                    for (var i : value.getStatements()) {
                        var s = i.getStatement();
                        if (readOnlyKey.getIsOutgoing() && s.getObjectLabel() != null) {
                            l.add(s.getObjectLabel());
                        } else if (s.getSubjectLabel() != null) {
                            l.add(s.getSubjectLabel());
                        }
                    }

                    var res = CommunityFieldTopLabelsValue.newBuilder()
                            .setTargetLabels(l)
                            .build();

                    if (value.getClassId() != null) {
                        res.setPropertyLabelId(CommunityPropertyLabelKey.newBuilder()
                                .setClassId(value.getClassId())
                                .setPropertyId(value.getPropertyId())
                                .setIsOutgoing(value.getIsOutgoing())
                                .setLanguageId(I.EN.get())
                                .build());
                    }

                    return res;
                },
                Materialized.<CommunityTopStatementsKey, CommunityFieldTopLabelsValue, KeyValueStore<Bytes, byte[]>>as(n2)
                        .withKeySerde(avroSerdes.CommunityTopStatementsKey())
                        .withValueSerde(avroSerdes.CommunityFieldTopLabelsValue())
        );

        // 3
        var n3 = "community_" + communitySlug + "_entity_fulltext_join_prop_label";
        var t = communityFieldTopLabelsTable.leftJoin(
                communityPropertyLabelTable,
                CommunityFieldTopLabelsValue::getPropertyLabelId,
                (value1, value2) -> FieldLabelWithTopLabelsValue.newBuilder()
                        .setPropertyId(value1.getPropertyLabelId().getPropertyId())
                        .setIsOutgoing(value1.getPropertyLabelId().getIsOutgoing())
                        .setTargetLabels(value1.getTargetLabels())
                        .setPropertyLabel(value2 != null ? value2.getLabel() != null ? value2.getLabel() : "" : "")
                        .build(),
                TableJoined.as(n3 + "-fk-left-join"),
                Materialized.<CommunityTopStatementsKey, FieldLabelWithTopLabelsValue, KeyValueStore<Bytes, byte[]>>as(n3)
                        .withKeySerde(avroSerdes.CommunityTopStatementsKey())
                        .withValueSerde(avroSerdes.FieldLabelWithTopLabelsValue())
        );

        // 4
        var n4a = "community_" + communitySlug + "_fulltext_fields_grouped_by_entity";
        var grouped = t
                .toStream()
                .groupBy((key, value) -> CommunityEntityKey.newBuilder()
                                .setEntityId(key.getEntityId())
                                .build(),
                        Grouped.with(
                                avroSerdes.CommunityEntityKey(), avroSerdes.FieldLabelWithTopLabelsValue()
                        ).withName(n4a)
                );

        var n4b = "community_" + communitySlug + "_fulltext_fields_aggregated_by_entity";
        var aggregated = grouped.aggregate(() -> EntityFieldTextMapValue.newBuilder().build(),
                (key, value, aggregate) -> {
                    var map = aggregate.getFields();
                    var k = FullTextFactory.getFieldKey(value.getIsOutgoing(), value.getPropertyId());
                    map.put(k, value);
                    aggregate.setFields(map);
                    return aggregate;
                },
                Materialized.<CommunityEntityKey, EntityFieldTextMapValue, KeyValueStore<Bytes, byte[]>>as(n4b)
                        .withKeySerde(avroSerdes.CommunityEntityKey())
                        .withValueSerde(avroSerdes.EntityFieldTextMapValue())
        );

        // 5
        var n5a = "community_" + communitySlug + "_entity_fulltext_label_config";
        var withConfig = aggregated.leftJoin(
                communityEntityWithLabelConfigTable,
                (value1, value2) -> EntityFieldTextMapWithConfigValue.newBuilder()
                        .setFields(value1.getFields())
                        .setLabelConfig(value2 != null && Utils.booleanIsNotEqualTrue(value2.getDeleted$1()) ? value2.getConfig() : null)
                        .build(),
                Named.as(n5a + "-fk-left-join"),
                Materialized.<CommunityEntityKey, EntityFieldTextMapWithConfigValue, KeyValueStore<Bytes, byte[]>>as(n5a)
                        .withKeySerde(avroSerdes.CommunityEntityKey())
                        .withValueSerde(avroSerdes.EntityFieldTextMapWithConfigValue())
        );

        var fulltextTable = withConfig.mapValues((readOnlyKey, value) -> CommunityEntityFulltextValue.newBuilder()
                .setFulltext(FullTextFactory.createFulltext(value))
                .setEntityId(readOnlyKey.getEntityId())
                .build());

        var n5b = "ktable-to-stream-community-" + communitySlug + "-fulltext";
        var fulltextStream = fulltextTable.toStream(Named.as(n5b));

        /* SINK PROCESSORS */

        fulltextStream.to(outputTopicNames.communityEntityFulltext(),
                Produced.with(avroSerdes.CommunityEntityKey(), avroSerdes.CommunityEntityFulltextValue())
                        .withName(outputTopicNames.communityEntityFulltext() + "-producer")
        );
    }
}