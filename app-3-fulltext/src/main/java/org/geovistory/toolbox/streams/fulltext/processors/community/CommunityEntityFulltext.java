package org.geovistory.toolbox.streams.fulltext.processors.community;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.fulltext.I;
import org.geovistory.toolbox.streams.fulltext.RegisterInputTopic;
import org.geovistory.toolbox.streams.fulltext.processors.FullTextFactory;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.LinkedList;


public class CommunityEntityFulltext {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder(), "toolbox").describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder, String nameSupplement) {
        var inputTopic = new RegisterInputTopic(builder);

        addProcessors(
                inputTopic.communityEntityWithLabelConfigTable(),
                inputTopic.communityTopStatementsTable(),
                inputTopic.communityPropertyLabelTable(),
                nameSupplement
        );

        return builder.build();
    }

    public static void addProcessors(
            KTable<CommunityEntityKey, CommunityEntityLabelConfigValue> communityEntityWithLabelConfigTable,
            KTable<CommunityTopStatementsKey, CommunityTopStatementsValue> communityTopStatementsTable,
            KTable<CommunityPropertyLabelKey, CommunityPropertyLabelValue> communityPropertyLabelTable,
            String nameSupplement
    ) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2
        var n2 = "community_" + nameSupplement + "field_top_labels_store";
        var communityFieldTopLabelsTable = communityTopStatementsTable.mapValues((readOnlyKey, value) -> {
                    var l = new LinkedList<String>();
                    for (var i : value.getStatements()) {
                        var s = i.getStatement();
                        if (readOnlyKey.getIsOutgoing()) {
                            l.add(s.getObjectLabel());
                        } else {
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
        var n3 = "community_" + nameSupplement + "_entity_fulltext_join_prop_label";
        var t = communityFieldTopLabelsTable.leftJoin(
                communityPropertyLabelTable,
                CommunityFieldTopLabelsValue::getPropertyLabelId,
                (value1, value2) -> FieldLabelWithTopLabelsValue.newBuilder()
                        .setPropertyId(value2.getPropertyId())
                        .setIsOutgoing(value2.getIsOutgoing())
                        .setPropertyLabel(value2.getLabel())
                        .setTargetLabels(value1.getTargetLabels())
                        .build(),
                TableJoined.as(n3 + "-fk-left-join"),
                Materialized.<CommunityTopStatementsKey, FieldLabelWithTopLabelsValue, KeyValueStore<Bytes, byte[]>>as(n3)
                        .withKeySerde(avroSerdes.CommunityTopStatementsKey())
                        .withValueSerde(avroSerdes.FieldLabelWithTopLabelsValue())
        );

        // 4
        var n4a = "community_" + nameSupplement + "_fulltext_fields_grouped_by_entity";
        var grouped = t
                .toStream()
                .groupBy((key, value) -> CommunityEntityKey.newBuilder()
                                .setEntityId(key.getEntityId())
                                .build(),
                        Grouped.with(
                                avroSerdes.CommunityEntityKey(), avroSerdes.FieldLabelWithTopLabelsValue()
                        ).withName(n4a)
                );

        var n4b = "community_" + nameSupplement + "_fulltext_fields_aggregated_by_entity";
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
        var n5a = "community_" + nameSupplement + "_entity_fulltext_label_config";
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

        var n5b = "ktable-to-stream-community-" + nameSupplement + "-fulltext";
        var fulltextStream = fulltextTable.toStream(Named.as(n5b));

        /* SINK PROCESSORS */

        fulltextStream.to(getOutputTopicName(nameSupplement),
                Produced.with(avroSerdes.CommunityEntityKey(), avroSerdes.CommunityEntityFulltextValue())
                        .withName(getOutputTopicName(nameSupplement) + "-producer")
        );


    }


    public static String getOutputTopicName(String nameSupplement) {
        return Utils.tsPrefixed("community_" + nameSupplement + "_entity_fulltext");
    }
}
