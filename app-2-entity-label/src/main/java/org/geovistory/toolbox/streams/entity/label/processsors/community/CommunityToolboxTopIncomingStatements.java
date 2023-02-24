package org.geovistory.toolbox.streams.entity.label.processsors.community;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label.RegisterInnerTopic;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.ArrayList;
import java.util.List;


public class CommunityToolboxTopIncomingStatements {
    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerOutputTopic = new RegisterInnerTopic(builder);

        return addProcessors(
                builder,
                registerOutputTopic.communityToolboxStatementWithEntityTable(),
                registerOutputTopic.communityToolboxEntityLabelTable()
        ).builder().build();
    }

    public static CommunityTopStatementsReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<CommunityStatementKey, CommunityStatementValue> communityToolboxStatementWithEntityTable,
            KTable<CommunityEntityKey, CommunityEntityLabelValue> communityToolboxEntityLabelTable) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)
        // join subject entity labels to get subject label
        var joinedSubjectEntityLabelsTable = communityToolboxStatementWithEntityTable.join(
                communityToolboxEntityLabelTable,
                communityToolboxStatementValue -> CommunityEntityKey.newBuilder()
                        .setEntityId(communityToolboxStatementValue.getStatement().getSubjectId())
                        .build(),
                (communityToolboxStatementValue, communityToolboxEntityLabelValue) -> {
                    if (communityToolboxEntityLabelValue != null && communityToolboxEntityLabelValue.getLabel() != null) {
                        communityToolboxStatementValue.getStatement().setSubjectLabel(communityToolboxEntityLabelValue.getLabel());
                    }
                    return communityToolboxStatementValue;
                },
                TableJoined.as(inner.TOPICS.community_toolbox_top_incoming_statements_join_subject_entity_label + "-fk-join"),
                Materialized.<CommunityStatementKey, CommunityStatementValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.community_toolbox_top_incoming_statements_join_subject_entity_label)
                        .withKeySerde(avroSerdes.CommunityStatementKey())
                        .withValueSerde(avroSerdes.CommunityStatementValue())
        );

        // 4
        var grouped = joinedSubjectEntityLabelsTable
                .toStream(
                        Named.as(inner.TOPICS.community_toolbox_top_incoming_statements_join_subject_entity_label + "-to-stream")
                )
                .groupBy(
                        (key, value) ->
                                CommunityTopStatementsKey.newBuilder()
                                        .setEntityId(value.getStatement().getObjectId())
                                        .setPropertyId(value.getStatement().getPropertyId())
                                        .setIsOutgoing(false)
                                        .build(),
                        Grouped
                                .with(avroSerdes.CommunityTopStatementsKey(), avroSerdes.CommunityStatementValue())
                                .withName(inner.TOPICS.community_toolbox_top_incoming_statements_group_by)
                );
        // 5
        var aggregatedTable = grouped.aggregate(
                () -> CommunityTopStatementsValue.newBuilder()
                        .setPropertyId(0)
                        .setEntityId("")
                        .setStatements(new ArrayList<>())
                        .setIsOutgoing(false)
                        .build(),
                (aggKey, newValue, aggValue) -> {
                    aggValue.setEntityId(aggKey.getEntityId());
                    aggValue.setPropertyId(aggKey.getPropertyId());
                    List<CommunityStatementValue> statements = aggValue.getStatements();
                    var newStatements = CommunityTopStatementAdder.addStatement(statements, newValue, false);
                    aggValue.setStatements(newStatements);
                    return aggValue;
                },
                Materialized.<CommunityTopStatementsKey, CommunityTopStatementsValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.community_toolbox_top_incoming_statements_aggregate)
                        .withKeySerde(avroSerdes.CommunityTopStatementsKey())
                        .withValueSerde(avroSerdes.CommunityTopStatementsValue())
        );


        var aggregatedStream = aggregatedTable.toStream(
                Named.as(inner.TOPICS.community_toolbox_top_incoming_statements_aggregate + "-to-stream")
        );

        /* SINK PROCESSORS */

        aggregatedStream.to(output.TOPICS.community_toolbox_top_incoming_statements,
                Produced.with(avroSerdes.CommunityTopStatementsKey(), avroSerdes.CommunityTopStatementsValue())
                        .withName(output.TOPICS.community_toolbox_top_incoming_statements + "-producer")
        );

        return new CommunityTopStatementsReturnValue(builder, aggregatedTable, aggregatedStream);

    }


    public enum input {
        TOPICS;
        public final String community_toolbox_statement_with_entity = CommunityToolboxStatementWithEntity.output.TOPICS.community_toolbox_statement_with_entity;
        public final String community_toolbox_entity_label = CommunityToolboxEntityLabel.output.TOPICS.community_toolbox_entity_label;
    }


    public enum inner {
        TOPICS;
        public final String community_toolbox_top_incoming_statements_group_by = "community_toolbox_top_incoming_statements_group_by";
        public final String community_toolbox_top_incoming_statements_aggregate = "community_toolbox_top_incoming_statements_aggregate";
        public final String community_toolbox_top_incoming_statements_join_subject_entity_label = "community_toolbox_top_incoming_statements_join_subject_entity_label";

    }

    public enum output {
        TOPICS;
        public final String community_toolbox_top_incoming_statements = Utils.tsPrefixed("community_toolbox_top_incoming_statements");
    }

}
