package org.geovistory.toolbox.streams.entity.processors.community;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.I;
import org.geovistory.toolbox.streams.entity.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.HashMap;


public class CommunityEntityTopStatements {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder(), "toolbox").describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder, String nameSupplement) {

        var registerInputTopics = new RegisterInputTopic(builder);

        return addProcessors(
                builder,
                registerInputTopics.communityEntityTable(),
                registerInputTopics.communityTopStatementsTable(),
                registerInputTopics.communityPropertyLabelTable(),
                nameSupplement
        ).builder().build();
    }

    public static CommunityEntityTopStatementsReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<CommunityEntityKey, CommunityEntityValue> communityEntityValueKTable,
            KTable<CommunityTopStatementsKey, CommunityTopStatementsValue> communityTopStatementsTable,
            KTable<CommunityPropertyLabelKey, CommunityPropertyLabelValue> communityPropertyLabelTable,
            String nameSupplement
    ) {

        var avroSerdes = new ConfluentAvroSerdes();

        String communityTopStatementsWithClassId = "community" + nameSupplement + "_top_statements_with_class_id";
        String communityTopStatementsWithPropLabel = "community" + nameSupplement + "_top_statements_with_prop_label";
        String communityEntityTopStatementsAggregated = "community" + nameSupplement + "_entity_top_statements_aggregated";

        /* STREAM PROCESSORS */
        // 2)
        // LeftJoin join the community entity with the top statements to add the entity class to CommunityTopStatementsWithClass
        var topStatementsWithClassTable = communityTopStatementsTable.leftJoin(
                communityEntityValueKTable,
                v -> CommunityEntityKey.newBuilder()
                        .setEntityId(v.getEntityId())
                        .build(),
                (value1, value2) -> {
                    if (value2 == null) return null;
                    return CommunityTopStatementsWithClassValue.newBuilder()
                            .setEntityId(value1.getEntityId())
                            .setIsOutgoing(value1.getIsOutgoing())
                            .setStatements(value1.getStatements())
                            .setPropertyId(value1.getPropertyId())
                            .setClassId(value2.getClassId())
                            .build();
                },
                TableJoined.as(communityTopStatementsWithClassId + "-fk-left-join"),
                Materialized.<CommunityTopStatementsKey, CommunityTopStatementsWithClassValue, KeyValueStore<Bytes, byte[]>>as(communityTopStatementsWithClassId)
                        .withKeySerde(avroSerdes.CommunityTopStatementsKey())
                        .withValueSerde(avroSerdes.CommunityTopStatementsWithClassValue())
        );

        // 3)
        // LeftJoin join the property label with the top statements to CommunityTopStatementsWithPropLabelValue
        var topStatementsWithPropLabelTable = topStatementsWithClassTable.leftJoin(
                communityPropertyLabelTable,
                v -> CommunityPropertyLabelKey.newBuilder()
                        .setClassId(v.getClassId())
                        .setIsOutgoing(v.getIsOutgoing())
                        .setPropertyId(v.getPropertyId())
                        .setLanguageId(I.EN.get())
                        .build(),
                (v, value2) -> CommunityTopStatementsWithPropLabelValue.newBuilder()
                        .setClassId(v.getClassId())
                        .setIsOutgoing(v.getIsOutgoing())
                        .setPropertyId(v.getPropertyId())
                        .setStatements(v.getStatements())
                        .setEntityId(v.getEntityId())
                        .setPropertyLabel(value2 != null ? value2.getLabel() : null)
                        .build(),
                TableJoined.as(communityTopStatementsWithPropLabel + "-fk-left-join"),
                Materialized.<CommunityTopStatementsKey, CommunityTopStatementsWithPropLabelValue, KeyValueStore<Bytes, byte[]>>as(communityTopStatementsWithPropLabel)
                        .withKeySerde(avroSerdes.CommunityTopStatementsKey())
                        .withValueSerde(avroSerdes.CommunityTopStatementsWithPropLabelValue())
        );

        // 4)
        // GroupBy CommunityEntityKey
        var groupedTable = topStatementsWithPropLabelTable
                .toStream(
                        Named.as(communityTopStatementsWithPropLabel + "-to-stream")
                )
                .groupBy(
                        (key, value) ->
                                CommunityEntityKey.newBuilder()
                                        .setEntityId(key.getEntityId())
                                        .build(),
                        Grouped.with(
                                avroSerdes.CommunityEntityKey(), avroSerdes.CommunityTopStatementsWithPropLabelValue()
                        ).withName("community_entity_top_statements_with_prop_label_grouped")
                );
        // 5)
        // Aggregate CommunityEntityTopStatementsValue, where the CommunityTopStatementKey is transformed to a string,
        // to be used as key in a map.
        var aggregatedTable = groupedTable.aggregate(
                () -> CommunityEntityTopStatementsValue.newBuilder()
                        .setClassId(0)
                        .setEntityId("")
                        .setMap(new HashMap<>())
                        .build(),
                (aggKey, newValue, aggValue) -> {
                    var key = newValue.getPropertyId() + "_" + (newValue.getIsOutgoing() ? "out" : "in");
                    if (newValue.getStatements().size() == 0) {
                        aggValue.getMap().remove(key);
                    } else {
                        aggValue.getMap().put(key, newValue);
                    }
                    return aggValue;
                },
                Named.as(communityEntityTopStatementsAggregated),
                Materialized.<CommunityEntityKey, CommunityEntityTopStatementsValue, KeyValueStore<Bytes, byte[]>>as(communityEntityTopStatementsAggregated)
                        .withKeySerde(avroSerdes.CommunityEntityKey())
                        .withValueSerde(avroSerdes.CommunityEntityTopStatementsValue())
        );


        var aggregatedStream = aggregatedTable.toStream(
                Named.as(communityEntityTopStatementsAggregated + "-to-stream")
        );

        /* SINK PROCESSORS */
        aggregatedStream.to(getOutputTopicName(nameSupplement),
                Produced.with(avroSerdes.CommunityEntityKey(), avroSerdes.CommunityEntityTopStatementsValue())
                        .withName(getOutputTopicName(nameSupplement) + "-producer")

        );

        return new CommunityEntityTopStatementsReturnValue(builder, aggregatedTable, aggregatedStream);

    }


    public static String getOutputTopicName(String nameSupplement) {
        return Utils.tsPrefixed("community_" + nameSupplement + "_entity_top_statements");
    }
}
