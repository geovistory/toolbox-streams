package org.geovistory.toolbox.streams.entity.preview.processors.community;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.preview.Klass;
import org.geovistory.toolbox.streams.entity.preview.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;


public class CommunityEntityPreview {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder(), "toolbox").describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder, String nameSupplement) {
        var inputTopic = new RegisterInputTopic(builder);

        return addProcessors(
                builder,
                inputTopic.communityEntityTable(),
                inputTopic.communityEntityLabelTable(),
                inputTopic.communityEntityClassLabelTable(),
                inputTopic.communityEntityTypeTable(),
                inputTopic.communityEntityTimeSpanTable(),
                inputTopic.communityEntityFulltextTable(),
                inputTopic.communityEntityClassMetadataTable(),
                nameSupplement
        ).builder().build();
    }

    public static CommunityEntityPreviewReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<CommunityEntityKey, CommunityEntityValue> communityEntityTable,
            KTable<CommunityEntityKey, CommunityEntityLabelValue> communityEntityLabelTable,
            KTable<CommunityEntityKey, CommunityEntityClassLabelValue> communityEntityClassLabelTable,
            KTable<CommunityEntityKey, CommunityEntityTypeValue> communityEntityTypeTable,
            KTable<CommunityEntityKey, TimeSpanValue> communityEntityTimeSpanTable,
            KTable<CommunityEntityKey, CommunityEntityFulltextValue> communityEntityFulltextTable,
            KTable<CommunityEntityKey, CommunityEntityClassMetadataValue> communityEntityClassMetadataTable,
            String nameSupplement
    ) {

        var avroSerdes = new ConfluentAvroSerdes();

        String communityEntityPreviewLabelJoin = "community_" + nameSupplement + "_entity_preview_label_join";
        String communityEntityPreviewClassLabelJoin = "community_" + nameSupplement + "_entity_preview_class_label_join";
        String communityEntityPreviewTypeJoin = "community_" + nameSupplement + "_entity_preview_type_join";
        String communityEntityPreviewTimeSpanJoin = "community_" + nameSupplement + "_entity_preview_time_span_join";
        String communityEntityPreviewFulltextJoin = "community_" + nameSupplement + "_entity_preview_fulltext_join";
        String communityEntityClassMetadataJoin = "community_" + nameSupplement + "_entity_class_metadata_join";
        /* STREAM PROCESSORS */
        // 2)

        var labelJoined = communityEntityTable.leftJoin(
                communityEntityLabelTable,
                (value1, value2) -> {
                    if (value1.getProjectCount() == 0) return null;
                    var newVal = EntityPreviewValue.newBuilder()
                            .setFkProject(null)
                            .setProject(0)
                            .setEntityId(value1.getEntityId())
                            .setPkEntity(parseStringId(value1.getEntityId()))
                            .setFkClass(value1.getClassId())
                            .setParentClasses("[]")
                            .setAncestorClasses("[]")
                            .setEntityType("")
                            .build();

                    if (value2 != null) newVal.setEntityLabel(value2.getLabel());

                    return newVal;
                },
                Named.as(communityEntityPreviewLabelJoin + "-left-join"),
                Materialized.<CommunityEntityKey, EntityPreviewValue, KeyValueStore<Bytes, byte[]>>as(communityEntityPreviewLabelJoin)
                        .withKeySerde(avroSerdes.CommunityEntityKey())
                        .withValueSerde(avroSerdes.EntityPreviewValue())
        );

        // 3
        var classLabelJoin = labelJoined.leftJoin(
                communityEntityClassLabelTable,
                (value1, value2) -> {
                    if (value2 != null) {
                        value1.setClassLabel(value2.getClassLabel());
                    }
                    return value1;
                },
                Named.as(communityEntityPreviewClassLabelJoin + "-left-join"),
                Materialized.<CommunityEntityKey, EntityPreviewValue, KeyValueStore<Bytes, byte[]>>as(communityEntityPreviewClassLabelJoin)
                        .withKeySerde(avroSerdes.CommunityEntityKey())
                        .withValueSerde(avroSerdes.EntityPreviewValue())
        );
        // 4
        var typeJoined = classLabelJoin.leftJoin(
                communityEntityTypeTable,
                (value1, value2) -> {
                    if (value2 != null) {
                        var typeId = value2.getTypeId();
                        if (typeId != null && typeId.length() > 1) {
                            value1.setTypeId(typeId);
                            value1.setFkType(parseStringId(typeId));
                        }
                        value1.setTypeLabel(value2.getTypeLabel());
                    }
                    return value1;
                },
                Named.as(communityEntityPreviewTypeJoin + "-left-join"),
                Materialized.<CommunityEntityKey, EntityPreviewValue, KeyValueStore<Bytes, byte[]>>as(communityEntityPreviewTypeJoin)
                        .withKeySerde(avroSerdes.CommunityEntityKey())
                        .withValueSerde(avroSerdes.EntityPreviewValue())
        );
        // 5
        var typeTimeSpan = typeJoined.leftJoin(
                communityEntityTimeSpanTable,
                (value1, value2) -> {
                    if (value2 != null) {
                        value1.setTimeSpan(value2.getTimeSpan().toString());
                        value1.setFirstSecond(value2.getFirstSecond());
                        value1.setLastSecond(value2.getLastSecond());
                    }
                    return value1;
                },
                Named.as(communityEntityPreviewTimeSpanJoin + "-left-join"),
                Materialized.<CommunityEntityKey, EntityPreviewValue, KeyValueStore<Bytes, byte[]>>as(communityEntityPreviewTimeSpanJoin)
                        .withKeySerde(avroSerdes.CommunityEntityKey())
                        .withValueSerde(avroSerdes.EntityPreviewValue())
        );
        // 6
        var typeFulltext = typeTimeSpan.leftJoin(
                communityEntityFulltextTable,
                (value1, value2) -> {
                    if (value2 != null) {
                        value1.setFullText(value2.getFulltext());
                    }
                    return value1;
                },
                Named.as(communityEntityPreviewFulltextJoin + "-left-join"),
                Materialized.<CommunityEntityKey, EntityPreviewValue, KeyValueStore<Bytes, byte[]>>as(communityEntityPreviewFulltextJoin)
                        .withKeySerde(avroSerdes.CommunityEntityKey())
                        .withValueSerde(avroSerdes.EntityPreviewValue())
        );

        // 7
        var classMetadata = typeFulltext.leftJoin(
                communityEntityClassMetadataTable,
                (value1, value2) -> {
                    if (value2 != null) {
                        var parents = value2.getParentClasses();
                        var ancestors = value2.getAncestorClasses();
                        value1.setParentClasses(parents.toString());
                        value1.setAncestorClasses(ancestors.toString());
                        var isPersistentItem = parents.contains(Klass.PERSISTENT_ITEM.get()) ||
                                ancestors.contains(Klass.PERSISTENT_ITEM.get());
                        var entityType = isPersistentItem ? "peIt" : "teEn";
                        value1.setEntityType(entityType);
                    }
                    return value1;
                },
                Named.as(communityEntityClassMetadataJoin + "-left-join"),
                Materialized.<CommunityEntityKey, EntityPreviewValue, KeyValueStore<Bytes, byte[]>>as(communityEntityClassMetadataJoin)
                        .withKeySerde(avroSerdes.CommunityEntityKey())
                        .withValueSerde(avroSerdes.EntityPreviewValue())
        );

        var communityEntityPreviewStream = classMetadata.toStream(
                Named.as(communityEntityClassMetadataJoin + "-to-stream")
        );

        /* SINK PROCESSORS */

        communityEntityPreviewStream
                .to(getOutputTopicName(nameSupplement),
                        Produced.with(avroSerdes.CommunityEntityKey(), avroSerdes.EntityPreviewValue())
                                .withName(getOutputTopicName(nameSupplement) + "-producer")
                );

        return new CommunityEntityPreviewReturnValue(builder, communityEntityPreviewStream);

    }

    private static int parseStringId(String value1) {
        try {
            return Integer.parseInt(value1.substring(1));
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static String getOutputTopicName(String nameSupplement) {
        return Utils.tsPrefixed("community_" + nameSupplement + "_entity_preview");
    }
}
